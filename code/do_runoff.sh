#!/usr/bin/env bash
#SBATCH --qos=nf
#SBATCH --job-name=get_N421_phyrnf
#SBATCH --time=00:10:00

# ==============================================================================
# Build runoff temperature forcing from ECMWF meteo (t2) for NEMO N421
# Usage:
#   sbatch do_runoff.sh YYYY MM DD
#
# Notes:
#   - Searches back ND_AGO days (default 4) for daily FORCE files and averages t2.
#   - If nothing is found, falls back to the current day FORCE file.
#   - Outputs river_data_t_yYYYYmMMdDD.nc in runoff_t_atmt2/.
# ==============================================================================

set -euo pipefail

# ------------------------------ config ----------------------------------------
OPT_CDO='-O -L -f nc4 -z zip_1'   # CDO output options
ND_AGO_DEFAULT=4

# Paths (adjust if needed)
WORKDIR="/ec/res4/hpcperm/eeim/nemo_ecmwf"
FORCING_ROOT="${WORKDIR}/forcing_N421"
METEODIR="${FORCING_ROOT}/meteo/meteo_nemo_ecmwf_BAL"
RUNOFFDIR="${FORCING_ROOT}/runoff"
DATADIR="${FORCING_ROOT}"                            # where bathy lives
BATHY_PATH="${DATADIR}/bathy_meter.nc"               # remap target

# ------------------------------ helpers ---------------------------------------
ts(){ TZ=EET date '+%Y-%m-%d %H:%M:%S (utc%z)'; }
die(){ echo "ERROR: $*" >&2; exit 1; }
need(){ command -v "$1" >/dev/null 2>&1 || die "Missing command: $1"; }

log(){ printf "[%s] %s\n" "$(ts)" "$*"; }

# ------------------------------ checks ----------------------------------------
need cdo
need date

# ------------------------------ args ------------------------------------------
if [[ $# -lt 3 ]]; then
  die "Usage: $0 YYYY MM DD"
fi

YY="$1"; MM="$2"; DD="$3"
# zero-pad month/day defensively
MM=$(printf '%02d' "$((10#$MM))")
DD=$(printf '%02d' "$((10#$DD))")

START_ISO="${YY}-${MM}-${DD}"
ND_AGO=${ND_AGO_DEFAULT}

log "do_runoff ${YY} ${MM} ${DD}  (lookback: ${ND_AGO} days)"

# ------------------------------ dirs ------------------------------------------
cd "${WORKDIR}"
ODIR="${RUNOFFDIR}/runoff_t_atmt2"
mkdir -p "${ODIR}"

# temp workspace
TMPDIR="$(mktemp -d "${ODIR}/tmp.XXXXXXXX")"
trap 'rm -rf "${TMPDIR}"' EXIT

[[ -s "${BATHY_PATH}" ]] || die "Bathy grid not found: ${BATHY_PATH}"

# ------------------------------ gather t2 means -------------------------------
have_any=0
t2_files=()

for d in $(seq 1 "${ND_AGO}"); do
  day_iso="$(date --date "${START_ISO} -${d} day" +%Y-%m-%d)"
  y=$(date -d "${day_iso}" +%Y)
  m=$(date -d "${day_iso}" +%m)
  d2=$(date -d "${day_iso}" +%d)

  force="${METEODIR}/${y}/${m}/${d2}/00/FORCE_ecmwf_y${y}m${m}d${d2}.nc"
  if [[ -s "${force}" ]]; then
    have_any=1
    out="${TMPDIR}/t2.${y}.${m}.${d2}.nc"
    log "Using meteo: ${force}"
    cdo ${OPT_CDO} timmean -selvar,t2 "${force}" "${out}"
    t2_files+=("${out}")
  else
    log "Missing meteo: ${force}"
  fi
done

if [[ "${have_any}" -eq 0 ]]; then
  # fallback to same day
  force="${METEODIR}/${YY}/${MM}/${DD}/00/FORCE_ecmwf_y${YY}m${MM}d${DD}.nc"
  [[ -s "${force}" ]] || die "No meteo FORCE file found for fallback: ${force}"
  out="${TMPDIR}/t2.${YY}.${MM}.${DD}.nc"
  log "Fallback to current day meteo: ${force}"
  cdo ${OPT_CDO} timmean -selvar,t2 "${force}" "${out}"
  t2_files=("${out}")
fi

# Merge time if multiple, else copy
MERGED="${TMPDIR}/t2_merged.nc"
if (( ${#t2_files[@]} > 1 )); then
  log "Merging ${#t2_files[@]} daily t2 means"
  cdo ${OPT_CDO} mergetime "${t2_files[@]}" "${MERGED}"
else
  cp -p "${t2_files[0]}" "${MERGED}"
fi

# ------------------------------ compute runoff temperature --------------------
# rotemp = max(t2 - 274.15, 0.10)  (in K -> offset to °C with floor at 0.10)
# Then remap bilinear to bathy grid and time-mean (in case multiple days merged)
OUTFILE="${ODIR}/river_data_t_y${YY}m${MM}d${DD}.nc"

log "Computing rotemp and remapping to bathy grid"
cdo ${OPT_CDO} \
  expr,"rotemp=(t2>=274.15)?t2-274.15:0.10" \
  -remapbil,"${BATHY_PATH}" \
  -timmean "${MERGED}" \
  "${OUTFILE}"

log "Wrote: ${OUTFILE}"
log "EOF do_runoff; timestamp: $(ts)"

