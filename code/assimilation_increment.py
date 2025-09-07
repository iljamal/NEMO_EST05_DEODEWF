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
    print(f"Using FORCINGDIR={FORCINGDIR}")
    output_file = os.path.join(output_dir, f"sla_state_cmems_{date_str}.nc")
    ds = fetch_sla_first_hour(date_str)
    remap_sla(ds, gridfile, output_file)

import numpy as np
#from scipy import ndimage

def fill_nan_with_nearest(data: xr.DataArray) -> xr.DataArray:
    """
    Fill NaNs in a 2D DataArray using nearest-neighbor approach.
    Input must be 2D (lat, lon).
    """
    import numpy as np
    from scipy import ndimage

    # Ensure 2D
    data_2d = data.squeeze()
    if data_2d.ndim != 2:
        raise ValueError(f"Expected 2D DataArray, got {data_2d.ndim}D")

    mask = np.isnan(data_2d)
    if not np.any(mask):
        return data_2d

    # Replace NaNs using nearest neighbor
    filled = data_2d.copy()
    filled_np = filled.data

    # Define a mask of valid values
    valid_mask = ~np.isnan(filled_np)
    filled_indices = ndimage.distance_transform_edt(~valid_mask,
                                                    return_distances=False,
                                                    return_indices=True)
    filled_np = filled_np[tuple(filled_indices)]

    # Return as a new DataArray with same coords
    return xr.DataArray(filled_np, coords=data_2d.coords, dims=data_2d.dims, attrs=data.attrs)


def generate_operational_ssh_increment(date_str):
    """
    Generate SSH assimilation increment as delta between
    EOF-reconstructed SSH and previous day NEMO output.
    """

    from pathlib import Path

    # Paths
    work_dir = os.environ.get("HPCPERM", os.getcwd()) + "/nemo_ecmwf"
    FORCINGDIR = os.environ.get("FORCINGDIR", f"{work_dir}/forcing")
    gridfile = os.path.join(FORCINGDIR, "bathy_meter.nc")
    output_dir = os.path.join(FORCINGDIR, "assim")

    rec_dir = f"{output_dir}/ssh_rec"
    gridfile = f"{FORCINGDIR}/bathy_meter.nc"

    date_obj = datetime.strptime(date_str, "%Y%m%d")
    prev_date = date_obj - timedelta(days=1)
    prev_date_str = prev_date.strftime("%Y%m%d")

    # --- Filenames
    scratch_dir= os.environ.get("SCRATCH", "/ec/res4/scratch/eeim/")
    nemo_file = f"{scratch_dir}NEMO5_EST_0.5nm_op_{prev_date_str}/EST05nm_op_rerun21_2ts_SSH_grid_T_{prev_date_str}-{prev_date_str}.nc"
    eof_file = f"{rec_dir}/ssh_rec.d{date_str}.t0000.nc"
    remapped_eof = f"{output_dir}/ssh_rec_remapped_{date_str}.nc"
    inc_file = f"{output_dir}/assim_background_increments.nc"

    # --- Step 1: Remap EOF SSH to model grid
    if not os.path.exists(eof_file):
        raise FileNotFoundError(f"Missing EOF reconstructed SSH: {eof_file}")
    cmd = (
        f"cdo -O -L -f nc4 -z zip2 "
        f"-fillmiss -setmisstonn -remapbil,{gridfile} "
        f"{eof_file} {remapped_eof}"
    )
    run_cmd(cmd)

     # --- Load datasets
    ds_rec = xr.open_dataset(remapped_eof)
    ds_mod = xr.open_dataset(nemo_file)

    # === EOF SSH ===
    ssh_rec = ds_rec['ssh']
    if 'time' in ssh_rec.dims:
        ssh_rec = ssh_rec.isel(time=0)

    ssh_rec = ssh_rec.squeeze()

    # Fill NaNs before using
    ssh_rec = fill_nan_with_nearest(ssh_rec)

    if 'time' in ssh_rec.dims:
        ssh_rec = ssh_rec.isel(time=0)
    ssh_rec = ssh_rec.squeeze()
    ssh_rec = ssh_rec.data  # Extract raw numpy array (2D)
  #  ssh_rec = fill_nan_with_nearest(ssh_rec)

    # === Model SSH
    ssh_mod = ds_mod['SSH']
    if 'time_counter' in ssh_mod.dims:
        ssh_mod = ssh_mod.isel(time_counter=-1)
    ssh_mod = ssh_mod.squeeze()
    ssh_mod = fill_nan_with_nearest(ssh_mod)
    if ssh_mod.ndim != 2:
        raise ValueError(f"Expected 2D model SSH, got shape: {ssh_mod.shape}")

    ssh_mod = ssh_mod.data  # convert to plain NumPy array


    # Final sanity: ensure it is 2D
    if ssh_mod.ndim != 2:
        raise ValueError(f"Model SSH still has {ssh_mod.ndim} dimensions: {ssh_mod.dims}")

    ssh_mod = ssh_mod.data  # Get raw 2D array

    # === Save for debug
    debug_file = f"/ec/res4/hpcperm/eeim/nemo_ecmwf/forcing/assim/debug_ssh_compare_{date_str}.nc"
    ds_debug = xr.Dataset(
        data_vars=dict(
            ssh_model=(["y", "x"], ssh_mod),
            ssh_eof=(["y", "x"], ssh_rec)
        ),
        coords=dict(
            y=np.arange(ssh_mod.shape[0]),
            x=np.arange(ssh_mod.shape[1])
        ),
        attrs=dict(description="Debug comparison of model vs EOF SSH")
    )
    ds_debug.to_netcdf(debug_file, format="NETCDF4_CLASSIC")
    print(f"[DEBUG] SSH model vs EOF saved to: {debug_file}")
    
    ssh_delta_np =ssh_rec-ssh_mod  # this is NumPy
    ssh_delta = xr.DataArray(ssh_delta_np[None, :, :], dims=["t", "y", "x"])
#    ssh_delta = fill_nan_with_nearest(ssh_delta)
    y_dim, x_dim = ssh_delta.shape[1], ssh_delta.shape[2]
    z_dim = 110
    nav_lev = np.arange(1, z_dim + 1)

    date_float = float(date_str)
    z_inc_datef = 20191203.5
    z_inc_dateb = date_float
    z_inc_daten = float(f"{date_str}.000002")
    time = float(f"{date_str}.000001")

    # === Write Assimilation Increment File ===
    work_dir = os.environ.get("HPCPERM", os.getcwd()) + "/nemo_ecmwf"
    FORCINGDIR = os.environ.get("FORCINGDIR", f"EEOR")
    inc_file = f"{FORCINGDIR}/assim/assim_background_increments.d{date_str}.nc"
    z_dim = 110
    nav_lev = np.arange(1, z_dim + 1)

    ds_out = xr.Dataset(
        data_vars=dict(
            bckineta=(["t", "y", "x"], ssh_delta.data),
            nav_lev=(["z"], nav_lev),
            z_inc_datef=([], z_inc_datef),
            z_inc_dateb=([], z_inc_dateb),
            z_inc_daten=([], z_inc_daten),
            time=([], time),
        ),
        coords=dict(
            t=[0],
            z=nav_lev,
            y=np.arange(ssh_delta.shape[1]),
            x=np.arange(ssh_delta.shape[2]),
        ),
        attrs=dict(description="Operational assimilation increment from SSH delta")
    )

    ds_out["bckineta"].attrs = {
        "long_name": "bckinetaIncrement",
        "units": "m",
        "coordinates": "nav_lat nav_lon",
        "_FillValue": 0.0,
        "missing_value": 0.0,
    }

    # Optional: add lat/lon if available
    if "latitude" in ds_rec and "longitude" in ds_rec:
        ds_out["nav_lat"] = (["y", "x"], ds_rec["latitude"].squeeze().data)
        ds_out["nav_lon"] = (["y", "x"], ds_rec["longitude"].squeeze().data)

    ds_out.to_netcdf(inc_file, format="NETCDF4_CLASSIC")
    print(f"[OK] [OPERATIONAL] Assimilation increment written to: {inc_file}")



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
    FORCINGDIR = os.environ.get("FORCINGDIR", f"../forcing")
    output_dir = f"{FORCINGDIR}/assim/"
    os.makedirs(output_dir, exist_ok=True)

    input_file = os.path.join(output_dir, f"sla_state_cmems_{date_str}.nc")
    inc_file = os.path.join(output_dir, f"assim_background_increments.d{date_str}.nc")
    di_file = os.path.join(output_dir, f"assim_background_state_DI.d{date_str}.nc")

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
    parser.add_argument("--mode", choices=["coldstart", "operational"], default="coldstart")
    args = parser.parse_args()

    if args.mode == "coldstart":
        generate_sla_increment(args.date)
        create_assim_background_files(args.date)
    elif args.mode == "operational":
        generate_operational_ssh_increment(args.date)