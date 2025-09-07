import os
import sys
import subprocess
from datetime import datetime
import copernicusmarine
import xarray as xr

def run_cmd(cmd):
    print(f"\nRunning: {cmd}")
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed: {cmd}")

def setup_cmems_credentials():
    username = os.getenv("CMEMS_USERNAME")
    password = os.getenv("CMEMS_PASSWORD")
    if not username or not password:
        raise EnvironmentError("CMEMS_USERNAME and CMEMS_PASSWORD must be set in the environment")
    return username, password

def get_env_path(varname, fallback):
    path = os.environ.get(varname)
    if not path:
        print(f"WARNING: {varname} not set. Falling back to: {fallback}")
        path = fallback
    return path

def init_case0_coldstart(yystart, mmstart, ddstart):
    date_str = f"{yystart}{mmstart}{ddstart}"
    print(f"==== Initializing cold start for {date_str} ====")

    # Ensure credentials are available
    setup_cmems_credentials()

    # Get environment paths
    FORCINGDIR = get_env_path("FORCINGDIR", "/ec/res4/hpcperm/eeim/nemo_ecmwf/forcing")
    CONFDIR    = get_env_path("CONFDIR",    "/ec/res4/hpcperm/eeim/nemo_ecmwf/setup_N5")
    print(f"Using FORCINGDIR={FORCINGDIR}")
    # Output paths
    data_dir = os.path.join(FORCINGDIR, "initial")
    print(f"Using data directory: {data_dir}")
    os.makedirs(data_dir, exist_ok=True)

    initfile = os.path.join(data_dir, f"initial_run_t{date_str}.nc")
    bathy_grid = os.path.join(FORCINGDIR, "bathy_meter.nc")

    # Download from CMEMS daily product
    ds = copernicusmarine.open_dataset(
        dataset_id="cmems_mod_bal_phy_anfc_P1D-m",
        start_datetime=f"{yystart}-{mmstart}-{ddstart}",
        end_datetime=f"{yystart}-{mmstart}-{ddstart}",
        variables=["so", "thetao"],
        minimum_longitude=19,
        maximum_longitude=31,
        minimum_latitude=56,
        maximum_latitude=61,
        minimum_depth=0,
        maximum_depth=140
    )
    file_path = os.path.join(data_dir, f"BAL-NEMO_PHY-DailyMeans-{date_str}.nc")
    ds.to_netcdf(file_path, format="NETCDF4_CLASSIC")
    print(f"Downloaded and saved CMEMS initial data: {file_path}")

    # Vertical levels
    zax = (
        "1.51,1.52,2.5,3.5,4.5,5.5,6.5,7.5,8.5,9.5,10.5,11.5,12.5,13.5,14.5,15.5,"
        "16.5,17.5,18.5,19.5,20.5,21.5,22.5,23.5,24.5,25.5,26.5,27.5,28.5,29.5,30.5,"
        "31.5,32.5,33.5,34.5,35.5,36.5,37.5,38.5,39.5,40.5,41.5,42.5,43.5,44.5,45.5,"
        "46.5,47.5,48.5,49.5,50.5,51.5,52.5,53.5,54.5,55.5,56.5,57.5,58.5,59.5,60.5,"
        "61.5,62.5,63.5,64.5,65.5,66.5,67.5,68.5,69.5,70.5,71.5,72.5,73.5,74.5,75.5,"
        "76.5,77.5,78.5,79.5,80.5,81.5,82.5,83.5,84.5,85.5,86.5,87.5,88.5,89.5,90.5,"
        "91.5,92.5,93.5,94.5,95.5,96.6,97.8,99.5,102,105.4,109.3,113.4,117.5,121.1,"
        "121.2,121.2,121.3,121.4,121.5"
    )

    # Interpolate and remap
    cmd = (
        f"cdo -s -O -L --reduce_dim intlevel,{zax} -setmisstonn "
        f"-remapbil,{bathy_grid} -expr,'so=so;thetao=thetao' "
        f"{file_path} {initfile}"
    )
    run_cmd(cmd)
    print(f"Initial file prepared: {initfile}")

def initialize_case(case_id, yystart, mmstart, ddstart):
    if case_id == 0:
        init_case0_coldstart(yystart, mmstart, ddstart)
    elif case_id == 2:
        print("Hotstart mode (case 2) — using restart files [not yet implemented]")
    elif case_id == 3:
        print("Assimilation mode (case 3) — call DA routines here")
    else:
        raise ValueError(f"Unknown initialization case: {case_id}")
