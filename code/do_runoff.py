import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import shutil

def run_cmd(cmd):
    print(f"Running: {cmd}")
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed: {cmd}")

def generate_runoff(date_str, ndays=1, lookback_days=4):
    """Generate runoff temperature forcing file using ECMWF t2 data."""
    # Parse date
    dt = datetime.strptime(date_str, "%Y%m%d")
    YY, MM, DD = dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")

    print(f"Generating runoff for {YY}-{MM}-{DD} with {lookback_days} day lookback")

    # Resolve paths
    FORCINGDIR = os.environ.get("FORCINGDIR", "/ec/res4/hpcperm/eeim/nemo_ecmwf/forcing")
    CONFDIR = os.environ.get("CONFDIR", "/ec/res4/hpcperm/eeim/nemo_ecmwf/setup_N5")
    METEODIR = f"{FORCINGDIR}/meteo/meteo_nemo_ecmwf_BAL"
    RUNOFFDIR = f"{FORCINGDIR}/runoff"
    BATHY_PATH = os.path.join(FORCINGDIR, "bathy_meter.nc")

    if not os.path.exists(BATHY_PATH):
        raise FileNotFoundError(f"Bathy grid not found: {BATHY_PATH}")

    out_dir = os.path.join(RUNOFFDIR, "runoff_t_atmt2")
    os.makedirs(out_dir, exist_ok=True)

    with tempfile.TemporaryDirectory(dir=out_dir) as tmpdir:
        t2_files = []
        found = False

        for d in range(1, lookback_days + 1):
            prev = dt - timedelta(days=d)
            y, m, dd = prev.strftime("%Y"), prev.strftime("%m"), prev.strftime("%d")
            ncfile = f"{METEODIR}/{y}/{m}/{dd}/00/FORCE_ecmwf_y{y}m{m}d{dd}.nc"
            if os.path.exists(ncfile):
                out = os.path.join(tmpdir, f"t2.{y}.{m}.{dd}.nc")
                print(f"Using meteo: {ncfile}")
                run_cmd(f"cdo -O -L -f nc4 -z zip_1 timmean -selvar,t2 {ncfile} {out}")
                t2_files.append(out)
                found = True
            else:
                print(f"Missing meteo: {ncfile}")

        if not found:
            # fallback to same day
            fallback = f"{METEODIR}/{YY}/{MM}/{DD}/00/FORCE_ecmwf_y{YY}m{MM}d{DD}.nc"
            if not os.path.exists(fallback):
                raise FileNotFoundError(f"No fallback meteo file found: {fallback}")
            out = os.path.join(tmpdir, f"t2.{YY}.{MM}.{DD}.nc")
            print(f"Fallback to current day: {fallback}")
            run_cmd(f"cdo -O -L -f nc4 -z zip_1 timmean -selvar,t2 {fallback} {out}")
            t2_files.append(out)

        merged = os.path.join(tmpdir, "t2_merged.nc")
        if len(t2_files) > 1:
            print(f"Merging {len(t2_files)} t2 means")
            run_cmd(f"cdo -O -L -f nc4 -z zip_1 mergetime {' '.join(t2_files)} {merged}")
        else:
            shutil.copy2(t2_files[0], merged)

        # Final output path
        outfile = f"{out_dir}/river_data_t_y{YY}m{MM}d{DD}.nc"
        print(f"Computing rotemp and remapping to bathy grid → {outfile}")
        expr = 'rotemp=(t2>=274.15)?t2-274.15:0.10'
        run_cmd(
            f"cdo -O -L -f nc4 -z zip_1 expr,\"{expr}\" "
            f"-remapbil,{BATHY_PATH} -timmean {merged} {outfile}"
        )
        print(f"Wrote: {outfile}")

