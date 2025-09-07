import os
from datetime import datetime, timedelta
import xarray as xr
import copernicusmarine
import subprocess
import numpy as np

def run_cmd(cmd):
    print(f"Running: {cmd}")
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed: {cmd}")
def setup_cmems_credentials():
    username = os.getenv("CMEMS_USERNAME")
    password = os.getenv("CMEMS_PASSWORD")
    if not username or not password:
        raise EnvironmentError("CMEMS_USERNAME and CMEMS_PASSWORD must be set in the environment")
    return username, password

def fetch_sla_first_hour(date_str):
    dt_start = datetime.strptime(date_str, "%Y%m%d")
    dt_end = dt_start + timedelta(hours=1)

    print(f"Fetching SLA for {dt_start.isoformat()} → {dt_end.isoformat()}")
    ds = copernicusmarine.open_dataset(
        dataset_id="cmems_mod_bal_phy_anfc_PT1H-i",
        start_datetime=dt_start.isoformat(),
        end_datetime=dt_end.isoformat(),
        variables=["sla"],
        minimum_longitude=19,
        maximum_longitude=30.2,
        minimum_latitude=56,
        maximum_latitude=61,
    )
    return ds

def remap_sla(ds, gridfile, output_path, cleanup=True):
    tmp_raw = "sla_raw.nc"
    tmp_remap = "sla_remap.nc"

    # Fix coordinate names for CDO
    if "latitude" in ds.coords and "longitude" in ds.coords:
        ds = ds.rename({"latitude": "lat", "longitude": "lon"})

    # Save raw file
    ds.to_netcdf(tmp_raw, format="NETCDF4_CLASSIC")
    print(f"[DEBUG] Saved {tmp_raw} with shape: {ds['sla'].shape}")

    # CDO remap
    cmd = f"cdo -O -L -f nc4 -z zip2 -setmisstonn -remapbil,{gridfile} {tmp_raw} {tmp_remap}"
    run_cmd(cmd)

    if not os.path.exists(tmp_remap):
        raise FileNotFoundError("CDO remap failed — output file 'sla_remap.nc' not found")
    try:
        ds_remap = xr.open_dataset(tmp_remap)
        sla_2d = ds_remap['sla'].isel(time=0)
    except Exception as e:
        raise RuntimeError(f"Failed to read remapped file: {e}")

    sla_2d.to_netcdf(output_path, format="NETCDF4_CLASSIC")
    print(f"Saved assimilation SLA field to: {output_path}")

#    if cleanup:
#        os.remove(tmp_raw)
#        os.remove(tmp_remap)



def generate_sla_increment(date_str):
    setup_cmems_credentials()  # Ensure credentials are set
    work_dir = os.environ.get("HPCPERM", os.getcwd()) + "/nemo_ecmwf"
    FORCINGDIR = os.environ.get("FORCINGDIR", f"{work_dir}/forcing")
    gridfile = os.path.join(FORCINGDIR, "bathy_meter.nc")
    output_dir = os.path.join(FORCINGDIR, "assim")
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"sla_state_cmems_{date_str}.nc")
    ds = fetch_sla_first_hour(date_str)
    remap_sla(ds, gridfile, output_file)

def create_assim_background_files(date_str):
    """
    Create both assimilation increment file and Direct Initialization file for NEMO.
    - assim_background_increments.nc : from CMEMS SLA
    - assim_background_state_DI.nc   : zero SSH and rdastp for DI mode

    Parameters
    ----------
    date_str : str
        Date in format 'YYYYMMDD', e.g. '20250701'
    """
    date_obj = datetime.strptime(date_str, "%Y%m%d")
    date_float = float(date_str)
    work_dir = os.environ.get("HPCPERM", os.getcwd()) + "/nemo_ecmwf"

    FORCINGDIR = os.environ.get("FORCINGDIR", f"{work_dir}/forcing")
    output_dir = os.path.join(FORCINGDIR, "assim")
    os.makedirs(output_dir, exist_ok=True)


    input_file = os.path.join(output_dir, f"sla_state_cmems_{date_str}.nc")
    inc_file = os.path.join(output_dir, "assim_background_increments.nc")
    di_file = os.path.join(output_dir, "assim_background_state_DI.nc")

    # === 1. Assimilation increment (from SLA) ===
    ds = xr.open_dataset(input_file)
    sla = ds["sla"].expand_dims(dim={"t": [0]})  # shape (t, y, x)

    y_dim, x_dim = sla.shape[1], sla.shape[2]
    z_dim = 110  # dummy depth dimension
    nav_lev = np.arange(1, z_dim + 1)

    z_inc_datef = 20191203.5
    z_inc_dateb = date_float
    z_inc_daten = float(f"{date_str}.000002")
    time = float(f"{date_str}.000001")
#    z_inc_daten = float(f"{date_str}")
#    time = float(f"{date_str}")

    out_inc = xr.Dataset(
        data_vars=dict(
            bckineta=(["t", "y", "x"], sla.values),
            nav_lev=(["z"], nav_lev),
            z_inc_datef=([], z_inc_datef),
            z_inc_dateb=([], z_inc_dateb),
            z_inc_daten=([], z_inc_daten),
            time=([], time),
        ),
        coords=dict(
            t=[0],
            z=nav_lev,
            y=np.arange(y_dim),
            x=np.arange(x_dim),
        ),
        attrs=dict(description="NEMO assimilation increment from CMEMS SLA")
    )

    # Optionally add nav_lat/nav_lon
    if "latitude" in ds and "longitude" in ds:
        out_inc["nav_lat"] = (["y", "x"], ds["latitude"].values)
        out_inc["nav_lon"] = (["y", "x"], ds["longitude"].values)

    # Attributes
    out_inc["bckineta"].attrs = {
        "long_name": "bckinetaIncrement",
        "units": "m",
        "coordinates": "nav_lat nav_lon",
        "_FillValue": 0.0,
        "missing_value": 0.0,
    }

    out_inc.to_netcdf(inc_file, format="NETCDF4_CLASSIC")
    print(f"[OK] Assimilation increment written to: {inc_file}")

    # === 2. Direct Initialization file ===
    sshn = np.zeros((y_dim, x_dim), dtype=np.float32)
    rdastp_value = float(f"{date_str}")

    out_di = xr.Dataset(
        data_vars=dict(
            sshn=(["y", "x"], sshn),
            rdastp=([], rdastp_value),
        ),
        coords=dict(
            y=np.arange(y_dim),
            x=np.arange(x_dim),
        ),
        attrs=dict(description="Direct Initialization background state file for NEMO")
    )

    # Add nav_lat/nav_lon if available
    if "latitude" in ds and "longitude" in ds:
        out_di["nav_lat"] = (["y", "x"], ds["latitude"].values)
        out_di["nav_lon"] = (["y", "x"], ds["longitude"].values)

    out_di["sshn"].attrs = {
        "long_name": "sea surface height",
        "units": "m",
        "_FillValue": 0.0,
        "missing_value": 0.0,
    }
    out_di["rdastp"].attrs = {
        "long_name": "assimilation date",
        "units": "YYYYMMDD.XXXX",
    }

    out_di.to_netcdf(di_file, format="NETCDF4_CLASSIC")
    print(f"[OK] Direct initialization background state written to: {di_file}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("date", help="Target date in YYYYMMDD")
    args = parser.parse_args()

    generate_sla_increment(args.date)
    create_assim_background_files(args.date)