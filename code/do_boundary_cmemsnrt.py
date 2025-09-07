import os
import sys
import argparse
from datetime import datetime, timedelta
import copernicusmarine
import xarray as xr
import subprocess

def run_cmd(cmd):
    print(f"Running: {cmd}")
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed: {cmd}")
def setup_cmems_credentials():
    username = os.getenv("CMEMS_USERNAME")
    password = os.getenv("CMEMS_PASSWORD")

    if not username or not password:
        # Optional: support config file fallback
        cred_file = os.path.expanduser("~/.cmems_credentials")
        if os.path.isfile(cred_file):
            with open(cred_file) as f:
                lines = f.readlines()
                username = lines[0].strip()
                password = lines[1].strip()
                os.environ["CMEMS_USERNAME"] = username
                os.environ["CMEMS_PASSWORD"] = password
                print(f"Using CMEMS credentials from {cred_file}")
        else:
            raise EnvironmentError("CMEMS_USERNAME and CMEMS_PASSWORD not set")

    return username, password
def fetch_boundary_data(start_date, ndays):
    start_dt = datetime.strptime(start_date, "%Y%m%d")
    end_dt = start_dt + timedelta(days=ndays + 1)

    # Define bounding box & variables
    region = dict(
        minimum_longitude=21.5,
        maximum_longitude=21.6,
        minimum_latitude=57.0,
        maximum_latitude=61.0,
        minimum_depth=0,
        maximum_depth=150
    )
    variables = ["sla", "thetao", "so", "siconc", "sithick"]
    dataset_id = "cmems_mod_bal_phy_anfc_PT1H-i"

    print(f"Fetching CMEMS data from {start_dt} to {end_dt}")
    ds = copernicusmarine.open_dataset(
        dataset_id=dataset_id,
        start_datetime=start_dt.isoformat(),
        end_datetime=end_dt.isoformat(),
        variables=variables,
        **region
    )
    return ds

def clean_for_cdo(ds):
    # Rename coords
    ds = ds.rename({"longitude": "lon", "latitude": "lat"})

    # Strip bad encodings and enforce CF-safe structure
    for var in ds.variables:
        if "_FillValue" in ds[var].encoding:
            ds[var].encoding["_FillValue"] = None
        ds[var].encoding["zlib"] = False  # CDO prefers uncompressed

    return ds

def save_to_netcdf(ds, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    # Rename coordinates to what CDO expects
#    ds = ds.rename({"longitude": "lon", "latitude": "lat"})
    # Transpose to enforce dimension order (needed for CDO compatibility)
 #   ds = ds.transpose("time", "lat", "lon", missing_dims="ignore")

    # Clean up encoding and fill values
#    cleaned = clean_for_cdo(ds)
    # Save NetCDF in a classic format
    cleaned=ds.copy(deep=True)
    cleaned.to_netcdf(path, format="NETCDF4_CLASSIC")
    print(f"Saved: {path}")

def generate_2d_bdy(remapped_file, out_2d_path):
    tmp_path = out_2d_path.replace(".nc", "_tmp.nc")
    cmd_expr = f"cdo -L -f nc4 -z zip2 expr,'sla=sla;uos=sla*0;vos=sla*0' {remapped_file} {tmp_path}"
    run_cmd(cmd_expr)

    # Load and reorder using xarray (instead of ncpdq)
    import xarray as xr
    ds = xr.open_dataset(tmp_path)
    ds = ds.transpose("time", "lon", "lat", missing_dims="ignore")
    ds.to_netcdf(out_2d_path, format="NETCDF4_CLASSIC")

#    os.remove(tmp_path)

def generate_3d_bdy(remapped_file, out_3d_path, zlevels):
    tmp_path = out_3d_path.replace(".nc", "_tmp.nc")
    cmd_expr = (
        f"cdo -L -f nc4 -z zip2 intlevel,{zlevels} "
        f"-expr,'thetao=thetao;so=so;vo=so*0;uo=so*0' {remapped_file} {tmp_path}"
    )
    run_cmd(cmd_expr)

    # Load and reorder using xarray
    import xarray as xr
    ds = xr.open_dataset(tmp_path)
    ds = ds.transpose("time", "depth", "lon", "lat", missing_dims="ignore")
    ds.to_netcdf(out_3d_path, format="NETCDF4_CLASSIC")

#    os.remove(tmp_path)

def remap_with_cdo(ncfile, gridfile, outdir, yyyymmdd):
    os.makedirs(outdir, exist_ok=True)
    base = os.path.splitext(os.path.basename(ncfile))[0]
    remap_nc = os.path.join(outdir, f"{base}_remap.nc")

    # 2D file
    cmd = f"cdo -L -s -O -f nc4 -z zip2 setmisstonn -remapbil,{gridfile} {ncfile} {remap_nc}"
    run_cmd(cmd)

    return remap_nc
from datetime import datetime, timedelta

def split_daily_2d_3d(tstr, ndays, odir):
    opt = "-L -s -O -f nc4 -z zip2"

    file_2d = os.path.join(odir, f"bdy_hourly_t144h_2d_{tstr}.nc")
    file_3d = os.path.join(odir, f"bdy_hourly_t144h_3d_{tstr}.nc")

    start_date = datetime.strptime(tstr, "%Y%m%d")

    for i in range(ndays):
        date_i = start_date + timedelta(days=i)
        yyyy = date_i.strftime("%Y")
        mm = date_i.strftime("%m")
        dd = date_i.strftime("%d")
        ymd_label = f"y{yyyy}m{mm}d{dd}"

        out_2d = os.path.join(odir, f"bdy_hourly_2d_{ymd_label}.nc")
        out_3d = os.path.join(odir, f"bdy_hourly_3d_{ymd_label}.nc")

        print(f"Creating daily files for {ymd_label}")

        cmd2d = f"cdo {opt} seldate,{yyyy}-{mm}-{dd} {file_2d} {out_2d}"
        cmd3d = f"cdo {opt} seldate,{yyyy}-{mm}-{dd} {file_3d} {out_3d}"

        run_cmd(cmd2d)
        run_cmd(cmd3d)

def run_teos10_conversion_on_3d_files(odir, tstr, ndays):
    start_date = datetime.strptime(tstr, "%Y%m%d")

    for i in range(ndays):
        date_i = start_date + timedelta(days=i)
        ymd_label = f"y{date_i.strftime('%Y')}m{date_i.strftime('%m')}d{date_i.strftime('%d')}"
        bdy3d_path = os.path.join(odir, f"bdy_hourly_3d_{ymd_label}.nc")
        bdy3d_out  = os.path.join(odir, f"bdy_hourly_3d_TEOS10_{ymd_label}.nc")
        print(f"Processing TEOS-10 for {bdy3d_path}  -> {bdy3d_out}")
        if os.path.exists(bdy3d_path):
            print(f"🔁 Converting to TEOS-10: {bdy3d_path}")
            cmd = f"python3 do_bdy3d_teos_conv.py {bdy3d_path} {bdy3d_out}"
 #           print(f"runing: {cmd}")
            run_cmd(cmd)
            # Optional: overwrite original with TEOS version
#            os.replace(bdy3d_out, bdy3d_path)
        else:
            print(f"⚠️  Skipping missing: {bdy3d_path}")

def run_physical_boundary(date_str, ndays):
    from datetime import datetime
    import os

    work_dir = os.environ.get("HPCPERM", os.getcwd()) + "/nemo_ecmwf"
    FORCINGDIR = os.environ.get("FORCINGDIR", f"{work_dir}/forcing")

    bdy_dir   = os.path.join(FORCINGDIR, "boundary")
    gridfile  = os.path.join(bdy_dir, "gridfile_bdy_est05")
    outdir    = os.path.join(bdy_dir, "cmems_nrt_bc_V110")
    rawfile   = os.path.join(bdy_dir, "cmems_nrt", "raw", f"bc_est_{date_str}.nc")


    print(f"==== STEP: Fetch Copernicus Baltic boundaries ====")
    ds = fetch_boundary_data(date_str, ndays)
    save_to_netcdf(ds, rawfile)
    remap_file = remap_with_cdo(rawfile, gridfile, outdir, date_str)

    tstr = date_str
    odir = os.path.join(outdir, date_str[:4], date_str[4:6], date_str[6:8], "00")
    os.makedirs(odir, exist_ok=True)

    out_2d_path = os.path.join(odir, f"bdy_hourly_t144h_2d_{tstr}.nc")
    out_3d_path = os.path.join(odir, f"bdy_hourly_t144h_3d_{tstr}.nc")

    zax = (
        "1.51,1.52,2.5,3.5,4.5,5.5,6.5,7.5,8.5,9.5,10.5,11.5,12.5,13.5,14.5,15.5,"
        "16.5,17.5,18.5,19.5,20.5,21.5,22.5,23.5,24.5,25.5,26.5,27.5,28.5,29.5,"
        "30.5,31.5,32.5,33.5,34.5,35.5,36.5,37.5,38.5,39.5,40.5,41.5,42.5,43.5,"
        "44.5,45.5,46.5,47.5,48.5,49.5,50.5,51.5,52.5,53.5,54.5,55.5,56.5,57.5,"
        "58.5,59.5,60.5,61.5,62.5,63.5,64.5,65.5,66.5,67.5,68.5,69.5,70.5,71.5,"
        "72.5,73.5,74.5,75.5,76.5,77.5,78.5,79.5,80.5,81.5,82.5,83.5,84.5,85.5,"
        "86.5,87.5,88.5,89.50001,90.50003,91.50011,92.5004,93.50152,94.50575,"
        "95.52176,96.5815,97.7951,99.46341,102.0134,105.4451,109.3315,"
        "114,114,114,114,114,114,114,114"
    )

    print(f"==== STEP: Post-process 2D/3D boundary files ====")
    generate_2d_bdy(remap_file, out_2d_path)
    generate_3d_bdy(remap_file, out_3d_path, zax)

    split_daily_2d_3d(tstr, ndays, odir)
        # TEOS-10 postprocessing
    print(f"==== STEP: TEOS-10 conversion on 3D files ====")    
    run_teos10_conversion_on_3d_files(odir, tstr, ndays)

    os.remove(out_2d_path)
    os.remove(out_3d_path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("date", help="Start date in YYYYMMDD")
    parser.add_argument("ndays", type=int, help="Forecast days")
    args = parser.parse_args()

    work_dir = os.environ.get("HPCPERM", os.getcwd()) + "/nemo_ecmwf"
    FORCINGDIR = os.environ.get("FORCINGDIR", f"{work_dir}/forcing")

    bdy_dir   = os.path.join(FORCINGDIR, "boundary")
    gridfile  = os.path.join(bdy_dir, "gridfile_bdy_est05")
    outdir    = os.path.join(bdy_dir, "cmems_nrt_bc_V110")
    rawfile   = os.path.join(bdy_dir, "cmems_nrt", "raw", f"bc_est_{args.date}.nc")


    print(f"==== STEP: Fetch Copernicus Baltic boundaries ====")
    ds = fetch_boundary_data(args.date, args.ndays)
    save_to_netcdf(ds, rawfile)

    print(f"==== STEP: Remap to NEMO grid ====")
    remap_file = remap_with_cdo(rawfile, gridfile, outdir, args.date)
    # Further processing: split 2D/3D, compute uos/vos, vo/uo placeholders etc
    # Output base name
    tstr = f"{args.date}"
    odir = os.path.join(outdir, args.date[:4], args.date[4:6], args.date[6:8], "00")
    os.makedirs(odir, exist_ok=True)

    out_2d_path = os.path.join(odir, f"bdy_hourly_t144h_2d_{tstr}.nc")
    out_3d_path = os.path.join(odir, f"bdy_hourly_t144h_3d_{tstr}.nc")

    # Define vertical levels (zax)
    zax = (
        "1.51,1.52,2.5,3.5,4.5,5.5,6.5,7.5,8.5,9.5,10.5,11.5,12.5,13.5,14.5,15.5,"
        "16.5,17.5,18.5,19.5,20.5,21.5,22.5,23.5,24.5,25.5,26.5,27.5,28.5,29.5,"
        "30.5,31.5,32.5,33.5,34.5,35.5,36.5,37.5,38.5,39.5,40.5,41.5,42.5,43.5,"
        "44.5,45.5,46.5,47.5,48.5,49.5,50.5,51.5,52.5,53.5,54.5,55.5,56.5,57.5,"
        "58.5,59.5,60.5,61.5,62.5,63.5,64.5,65.5,66.5,67.5,68.5,69.5,70.5,71.5,"
        "72.5,73.5,74.5,75.5,76.5,77.5,78.5,79.5,80.5,81.5,82.5,83.5,84.5,85.5,"
        "86.5,87.5,88.5,89.50001,90.50003,91.50011,92.5004,93.50152,94.50575,"
        "95.52176,96.5815,97.7951,99.46341,102.0134,105.4451,109.3315,"
        "114,114,114,114,114,114,114,114"
    )

    print(f"==== STEP: Post-process 2D/3D boundary files ====")
    generate_2d_bdy(remap_file, out_2d_path)
    generate_3d_bdy(remap_file, out_3d_path, zax)
        # TEOS-10 postprocessing
    print(f"==== STEP: TEOS-10 conversion on 3D files ====")    
    run_teos10_conversion_on_3d_files(odir, tstr, args.ndays)

    # Daily split
    print(f"==== STEP: split daily ====")
    split_daily_2d_3d(tstr, args.ndays, odir)

    # Optionally remove bulk files
#    os.remove(out_2d_path)
#    os.remove(out_3d_path)
    # (can port line-by-line from shell if needed)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("date", help="Start date in YYYYMMDD")
    parser.add_argument("ndays", type=int, help="Forecast days")
    args = parser.parse_args()
    run_physical_boundary(args.date, args.ndays)
