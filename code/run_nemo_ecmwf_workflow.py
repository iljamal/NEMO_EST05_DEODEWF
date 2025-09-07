import argparse
import subprocess
import sys
import os
from datetime import datetime, timedelta
from do_meteo_ecmwf import generate_meteo_ecmwf
from nemo_model_runner import NemoModelRunner
from do_boundary_cmemsnrt import run_physical_boundary
from init_startup import initialize_case
from do_runoff import generate_runoff
from assimilation_increment import (
    generate_sla_increment,
    create_assim_background_files,
    generate_operational_ssh_increment,
)

def run_command(cmd):
    print(f"\nRunning: {cmd}")
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        print(f"ERROR: Command failed: {cmd}")
        sys.exit(1)

#def generate_runoff(date_str, ndays):
#    print(f"(placeholder) Runoff generation for {date_str}, {ndays} days")
#    # Implement or import actual runoff generator here

def main():
    parser = argparse.ArgumentParser(description="Unified NEMO ECMWF workflow with spin-up date shift")
    parser.add_argument("--date", required=True, help="Target start date in YYYYMMDD format")
    parser.add_argument("--ndays", required=True, type=int, help="Forecast length in days")
    args = parser.parse_args()

    # Adjust workflow date to args.date - 1 day
    try:
        input_date = datetime.strptime(args.date, "%Y%m%d")
        start_date = input_date - timedelta(days=1)
    except ValueError:
        print("ERROR: Invalid date format. Use YYYYMMDD.")
        sys.exit(1)

    date_str = start_date.strftime("%Y%m%d")
    yyyy = start_date.strftime("%Y")
    mm = start_date.strftime("%m")
    dd = start_date.strftime("%d")

    print(f"Adjusted workflow: Processing date {date_str} (original input was {args.date})")

    # Define shared environment variables
    FORCINGDIR = "/ec/res4/hpcperm/eeim/deode_sswf/forcing"
    CONFDIR = "/ec/res4/hpcperm/eeim/deode_sswf/setup_N5"
    RUNDIR = "/ec/res4/scratch/eeim/"
    
    os.environ["FORCINGDIR"] = FORCINGDIR
    os.environ["CONFDIR"] = CONFDIR
    os.environ["RUNDIR"] = RUNDIR

        # === ECMWF OPERATIONAL WORKFLOW ===
    if date_str == "20241118":
        print("\n===== STEP 1: initialize (coldstart) =====")
#        initialize_case(case_id=0, yystart=yyyy, mmstart=mm, ddstart=dd)
        try:
            print("\n===== STEP 1.1: Assimilation Increment (CMEMS SLA) =====")
#            generate_sla_increment(date_str)
       #     create_assim_background_files(date_str)
        except Exception as e:
            print(f"WARNING: Assimilation increment step failed: {e}")
    else:
        try:
            print("\n===== STEP 1.2: Assimilation Increment (Operational SSH delta) =====")
            generate_operational_ssh_increment(date_str)
        except Exception as e:
            print(f"WARNING: Operational SSH increment step failed: {e}")
#    sys.exit(1)
    print("\n===== STEP 2: Physical boundary (Copernicus Marine) =====")
    run_physical_boundary(date_str, args.ndays)

    print("\n===== STEP 3: ECMWF meteo forcing =====")
#    generate_meteo_ecmwf(date_str, args.ndays)
    print("\n===== STEP 4: Runoff forcing =====")
#    generate_runoff(date_str, args.ndays)
    print("\n===== STEP 5: Run NEMO model =====")
#    sys.exit(1)
    runner = NemoModelRunner(yystart=yyyy, mmstart=mm, ddstart=dd, ndays=args.ndays)
    runner.full_run()
    
    print("\n===== SPINUP completed =====")
    sys.exit(1)

    # === DEODE SIMULATION WORKFLOW ===
    print("\n===== DEODE WORKFLOW START =====")

    print("\n===== STEP DEODE 6: Custom DEODE Meteo Forcing =====")
    # TODO: Replace with actual function
    # generate_deode_meteo_forcing(args.date, args.ndays)

    print("\n===== STEP DEODE 7: Run NEMO for DEODE =====")
    # TODO: Replace with correct runner
    # runner = NemoModelRunnerDeode(yystart=yyyy, mmstart=mm, ddstart=dd, ndays=args.ndays)
    # runner.full_run()

    print("\n===== STEP DEODE 8: do_DEODE =====")
    # TODO: Replace with actual function
    # generate_deode_meteo_forcing(args.date, args.ndays)

    print("\n===== ALL DONE SUCCESSFULLY =====")

if __name__ == "__main__":
    main()
