import os
import sys
import argparse
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

try:
    from ecmwfapi import ECMWFService
except ImportError:
    ECMWFService = None

def run_cmd(cmd: str):
    print(f"Running: {cmd}")
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed: {cmd}")

def ecmwf_det(date_str: str, ndays: int, area: str = "66/9/53/31", grid: str = ".08/.08"):
    if ECMWFService is None:
        raise RuntimeError("ecmwfapi not available. Load ECMWF env or install it locally.")

    y, m, d = date_str[:4], date_str[4:6], date_str[6:8]

    work_dir = os.environ.get("HPCPERM", os.getcwd()) + "/nemo_ecmwf"
    FORCINGDIR = os.environ.get("FORCINGDIR", f"{work_dir}/forcing")
#    print(f"Using FORCINGDIR={FORCINGDIR}")
    out_root = f"{FORCINGDIR}/ECMWF_fc"
    meteodir = FORCINGDIR
    temp_dir = f"{out_root}/temp"
    Path(temp_dir).mkdir(parents=True, exist_ok=True)

    max_hour = ndays * 24
    if max_hour <= 90:
        steps = "/".join(str(i) for i in range(0, max_hour + 1))
    elif max_hour <= 144:
        steps = "/".join(map(str, list(range(0, 91)) + list(range(93, max_hour + 1, 3))))
    else:
        steps = "/".join(map(str, list(range(0, 91)) + list(range(93, 145, 3)) + list(range(150, max_hour + 1, 6))))

    grib_file = f"{temp_dir}/FC_allsteps_d{date_str}ltd{ndays}.0000"
    if not Path(grib_file).exists():
        ECMWFService("mars").execute({
            "class": "od", "stream": "oper", "expver": "0001",
            "domain": "g", "type": "fc", "levtype": "sfc",
            "date": f"{y}-{m}-{d}", "time": "00", "step": steps,
            "use": "bc", "area": area, "grid": grid,
            "param": "2t/10u/10v/tp/msl/2d/sp/sf/ssrd/strd/tcc",
            "format": "grib2"
        }, grib_file)
    else:
        print(f"GRIB already exists: {grib_file}")

    odir = f"{meteodir}/meteo/meteo_nemo_ecmwf_BAL/{y}/{m}/{d}/00"
    Path(f"{odir}/temp").mkdir(parents=True, exist_ok=True)
    td = f"{odir}/temp"

    ifile = f"{td}/NES_allsteps_merge.nc"
    run_cmd(" ".join([
        "cdo -O -L -f nc4 -z zip_1",
        "chname,var167,t2,var165,u10,var166,v10,var151,msl,var134,sp,var168,d2,var169,ssrd,var175,strd,var228,tp,var164,tcc,var144,sf",
        f"-inttime,{y}-{m}-{d},00:00:00,1hour {grib_file} {ifile}"
    ]))

    q_expr = (
        "(0.622*(6.112*exp((17.67*(d2-273.15))/(d2-273.15+243.5)))) / "
        "(sp-(0.378*(6.112*exp((17.67*(d2-273.15))/(d2-273.15+243.5)))))*100"
    )
    run_cmd(f"cdo -O -L -f nc4 -z zip_1 expr,\"slp=msl;sh={q_expr};t2=t2;u10=u10;v10=v10;\" {ifile} {td}/meteo1.nc")

    run_cmd(f"cdo -O -L -f nc4 -z zip_1 expr,\"lwr=strd/3600;swr=ssrd/3600;tp=1000*tp/3600;snow=1000*sf/3600\" {ifile} {td}/meteo2_expr.nc")
    run_cmd(f"cdo -O -L -f nc4 -z zip_1 deltat {td}/meteo2_expr.nc {td}/meteo2_deltat.nc")

    prev_date = (datetime.strptime(date_str, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d")
    yym1, mmm1, ddm1 = prev_date[:4], prev_date[4:6], prev_date[6:8]
    prev_force = f"{meteodir}/meteo_nemo_ecmwf_BAL/{yym1}/{mmm1}/{ddm1}/00/FORCE_ecmwf_y{y}m{m}d{d}.nc"

    step1 = f"{td}/meteo2_step1.nc"
    if Path(prev_force).exists():
        run_cmd(f"cdo -O -L -f nc4 -z zip_1 expr,\"lwr=lwr;swr=swr;tp=tp;snow=snow\" -seltimestep,1 {prev_force} {step1}")
    else:
        run_cmd(f"cdo -O -L -f nc4 -z zip_1 -settaxis,{y}-{m}-{d},00:00:00,1hour -seltimestep,2 {td}/meteo2_expr.nc {step1}")

    run_cmd(f"cdo -O -L -f nc4 -z zip_1 mergetime {step1} {td}/meteo2_deltat.nc {td}/meteo2.nc")
    run_cmd(f"cdo -O -L -f nc4 -z zip_1 merge {td}/meteo1.nc {td}/meteo2.nc {td}/meteo_M.nc")

    for n in range(min(ndays, 15) + 1):
        day = (datetime.strptime(date_str, "%Y%m%d") + timedelta(days=n))
        yyyy2, mm2, dd2 = day.strftime("%Y"), day.strftime("%m"), day.strftime("%d")
        out = f"{odir}/FORCE_ecmwf_y{yyyy2}m{mm2}d{dd2}.nc"
        run_cmd(f"cdo -O -L -f nc4 -z zip_1 seldate,{yyyy2}-{mm2}-{dd2} {td}/meteo_M.nc {out}")

    for fname in ["NES_allsteps_merge.nc", "meteo1.nc", "meteo2_expr.nc", "meteo2_deltat.nc", "meteo2_step1.nc", "meteo2.nc", "meteo_M.nc"]:
        try:
            Path(f"{td}/{fname}").unlink()
        except FileNotFoundError:
            continue
    try:
        Path(td).rmdir()
    except OSError:
        pass

def generate_meteo_ecmwf(date_str: str, ndays: int):
    print(f"== METEO ECMWF forcing generation for {date_str} ({ndays} days) ==")
    ecmwf_det(date_str, ndays)

def cli():
    ap = argparse.ArgumentParser(description="ECMWF -> NEMO meteo forcing (MARS API)")
    ap.add_argument("--mode", default="ecmwf_det", choices=["ecmwf_det"], help="Data source/method")
    ap.add_argument("--date", required=True, help="Start date YYYYMMDD")
    ap.add_argument("--ndays", type=int, required=True, help="Forecast length in days")
    args = ap.parse_args()

    if args.mode == "ecmwf_det":
        generate_meteo_ecmwf(args.date, args.ndays)

if __name__ == "__main__":
    cli()
