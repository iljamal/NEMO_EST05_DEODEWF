#!/usr/bin/env python3
"""
Fetch DEODE (HARMONIE-AROME) meteorology GRIBs to $SCRATCH for NEMO pre-processing.

Copies files via `ecp` from the ECMWF filesystem path:
  ec:/fie/deode/CY49t2_HARMONIE_AROME_FIN_S_16Nov2024_1500x1500_500m_2024_11_v3/archive/YYYY/MM/DD/CC/mbr000/
pattern:
  GRIBPFDEOD+*

Destination:
  $SCRATCH/deode/meteoprep/<case_str>/

Usage:
  python do_meteo_deode_fetch.py --date 20241115 \
      [--cycle 00] [--case-str 20241115] \
      [--dest-root $SCRATCH/deode/meteoprep] \
      [--member mbr000]

Notes:
- This script only performs the COPY step. Conversion to NEMO forcing is separate.
- Requires `ecp` in PATH and permissions to read from the source path.
"""
import os
import argparse
import subprocess
from pathlib import Path
from datetime import datetime

BASE_ROOT = (
    "ec:/fie/deode/"
    "CY49t2_HARMONIE_AROME_FIN_S_16Nov2024_1500x1500_500m_2024_11_v3/archive"
)

def cli():
    ap = argparse.ArgumentParser(description="Copy DEODE meteo GRIBs to $SCRATCH")
    ap.add_argument("--date", required=True, help="YYYYMMDD")
    ap.add_argument("--cycle", default="00", help="Cycle hour, e.g., 00/06/12/18")
    ap.add_argument("--case-str", default=None, help="Case label for destination folder (default: YYYYMMDD)")
    ap.add_argument("--dest-root", default=None, help="Destination root (default: $SCRATCH/deode/meteoprep)")
    ap.add_argument("--member", default="mbr000", help="Member subfolder name")
    args = ap.parse_args()

    date_str = args.date
    cycle = args.cycle
    case_str = args.case_str or date_str
    dest_root = args.dest_root or os.path.join(os.environ.get("SCRATCH", "/tmp"), "deode", "meteoprep")
    member = args.member

    dt = datetime.strptime(date_str, "%Y%m%d")
    yyyy, mm, dd = dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")

    src_dir = f"{BASE_ROOT}/{yyyy}/{mm}/{dd}/{cycle}/{member}"
    out_dir = os.path.join(dest_root, case_str)
    os.makedirs(out_dir, exist_ok=True)

    cmd = f"ecp {src_dir}/GRIBPFDEOD+* {out_dir}/"
    print(f"Running: {cmd}")
    subprocess.run(cmd, shell=True, check=True)
    print(f"Done. Files copied to: {out_dir}")

if __name__ == "__main__":
    cli()
