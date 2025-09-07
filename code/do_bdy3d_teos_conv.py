#!/usr/bin/env python3
"""
Convert boundary file (thetao, so) from practical salinity / potential temperature
to TEOS-10 Absolute Salinity (SA) and Conservative Temperature (CT).

- Expects 4-D variables shaped like (time, depth, Y, X); typical boundary files have Y=1.
- Pressure is estimated as p[dbar] = depth[m] * 0.1.
- lon/lat are taken from the dataset if present; otherwise you can provide --lon/--lat.
- Writes compressed NetCDF4 with updated attributes.

Usage:
  python do_bdy3d_teos_conv.py INPUT.nc OUTPUT.nc [--lon 24.0] [--lat 60.0]
"""

import argparse
import sys
import numpy as np
import xarray as xr
import gsw


def find_first(container, names):
    """Return the first present name from 'names' in 'container' (mapping or sequence)."""
    for n in names:
        if n in container:
            return n
    return None


def die(msg: str) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)


def main():
    ap = argparse.ArgumentParser(description="Convert SP/thetao to SA/CT (TEOS-10).")
    ap.add_argument("input", help="Input NetCDF")
    ap.add_argument("output", help="Output NetCDF")
    ap.add_argument("--lon", type=float, default=None, help="Fallback longitude (degE)")
    ap.add_argument("--lat", type=float, default=None, help="Fallback latitude (degN)")
    args = ap.parse_args()

    ds = xr.open_dataset(args.input)

    # --- Locate variables
    theta_name = find_first(ds, ["thetao", "theta", "pt", "potemp"])
    so_name    = find_first(ds, ["so", "SP", "salinity", "practical_salinity"])
    depth_name = find_first(ds, ["depth", "deptht", "z", "lev"])

    if not theta_name or not so_name or not depth_name:
        die(f"Missing required variables; found: theta={theta_name}, so={so_name}, depth={depth_name}")

    tp = ds[theta_name]  # potential temperature (°C)
    sp = ds[so_name]     # practical salinity (unitless)
    depth = ds[depth_name].astype("float64")

    if tp.ndim != 4 or sp.ndim != 4:
        die(f"Expected 4D arrays; got theta dims={tp.dims}, so dims={sp.dims}")

    # --- Identify axes (assume (time, depth, Y, X); be tolerant on names)
    dims = list(sp.dims)
    try:
        time_ax = dims.index(find_first(dims, ["time"]))
    except ValueError:
        time_ax = 0  # fallback

    try:
        depth_ax = dims.index(depth_name)
    except ValueError:
        depth_ax = 1  # fallback

    lon_ax_name = find_first(dims, ["lon", "longitude", "x"])
    lon_ax = dims.index(lon_ax_name) if lon_ax_name else 3

    # lat axis is "the other one"
    lat_ax_candidates = [i for i in range(4) if i not in (time_ax, depth_ax, lon_ax)]
    if not lat_ax_candidates:
        die("Could not determine latitude axis.")
    lat_ax = lat_ax_candidates[0]

    shp = sp.shape
    T, Z, Y, X = [shp[i] for i in (time_ax, depth_ax, lat_ax, lon_ax)]

    # --- Build pressure (broadcast to full SP shape)
    # depth is 1-D (Z,); make an array with shape (1, Z, 1, 1) then broadcast
    p1d = (np.asarray(depth) * 0.1)  # dbar
    p_shape = [1, 1, 1, 1]
    p_shape[depth_ax] = Z
    p4 = p1d.reshape(p_shape)
    p = np.broadcast_to(p4, shp)

    # --- lon/lat
    # Prefer variables from ds; fall back to flags; ensure we end up with:
    #   lon: line along X (length X), broadcast to full shape
    #   lat: scalar (or 1-long Y) broadcastable to full shape
    lon_var_name = find_first(ds, ["lon", "longitude", "nav_lon", "x"])
    lat_var_name = find_first(ds, ["lat", "latitude", "nav_lat", "y"])

    lon_line = None
    lat0 = None

    if lon_var_name in ds:
        lonv = np.asarray(ds[lon_var_name])
        if lonv.ndim == 1 and lonv.shape[0] >= X:
            lon_line = lonv[:X].astype("float64")
        elif lonv.ndim == 2 and lonv.shape[-1] >= X:
            lon_line = lonv[0, :X].astype("float64")
    if lon_line is None:
        if args.lon is None:
            # conservative fallback; adjust if your domain is different
            lon_line = np.full((X,), 24.0, dtype="float64")
        else:
            lon_line = np.full((X,), float(args.lon), dtype="float64")

    if lat_var_name in ds:
        latv = np.asarray(ds[lat_var_name])
        if latv.ndim == 0:
            lat0 = float(latv)
        elif latv.ndim == 1:
            lat0 = float(latv[0])
        else:
            lat0 = float(latv[0, 0])
    if lat0 is None:
        lat0 = 60.0 if args.lat is None else float(args.lat)

    # Reshape lon to broadcast along lon axis only
    lon_shape = [1, 1, 1, 1]
    lon_shape[lon_ax] = X
    lon4 = lon_line.reshape(lon_shape)
    # lat as scalar is fine for broadcasting; keep it scalar
    lat_scalar = float(lat0)

    # --- Prepare SP and TP arrays
    spv = np.asarray(sp, dtype="float64")
    tpv = np.asarray(tp, dtype="float64")

    # --- Compute SA and CT (broadcasting does the rest)
    # gsw.SA_from_SP accepts arrays or scalars for lon/lat
    SA = gsw.SA_from_SP(spv, p, lon4, lat_scalar)
    CT = gsw.CT_from_pt(SA, tpv)

    if SA.shape != spv.shape or CT.shape != tpv.shape:
        die(f"Computed shapes do not match inputs: SA{SA.shape} vs SP{spv.shape}, CT{CT.shape} vs TP{tpv.shape}")

    # --- Write back with attributes
    dso = ds.copy()

    dso[so_name].data = SA
    dso[so_name].attrs.update({
        "long_name": "Absolute Salinity",
        "standard_name": "sea_water_absolute_salinity",
        "units": "g kg-1",
        "comment": "Computed from practical salinity via TEOS-10 (GSW)."
    })

    dso[theta_name].data = CT
    dso[theta_name].attrs.update({
        "long_name": "Conservative Temperature",
        "standard_name": "sea_water_conservative_temperature",
        "units": "degC",
        "comment": "Computed from potential temperature via TEOS-10 (GSW)."
    })

    # Compressed NetCDF4 output
    comp = dict(zlib=True, complevel=1, shuffle=True)
    encoding = {v: comp for v in dso.data_vars}
    dso.to_netcdf(args.output, encoding=encoding)

    # Sanity ping
    print("Sample SP->SA:", float(spv[(0, 0, 0, 0)]), "->", float(SA[(0, 0, 0, 0)]))
    print("Wrote:", args.output)
    print("end of TEOS-10 conversion")


if __name__ == "__main__":
    main()

