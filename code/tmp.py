def compute_and_dump_boundary_delta(date_str):
    """
    Regrid EOF reconstruction to boundary grid and compare to CMEMS boundary SLA.
    Dump both strips to NetCDF for validation.
    """
    import os
    import xarray as xr
    import numpy as np

    work_dir = os.environ.get("HPCPERM", os.getcwd()) + "/nemo_ecmwf"
    bdy_dir = f"{work_dir}/forcing/boundary"
    gridfile = f"{bdy_dir}/gridfile_bdy_est05"
    output_dir = f"/ec/res4/hpcperm/eeim/nemo_ecmwf/forcing/assim/"
    date_fmt = f"d{date_str}"

    # === File paths ===
    bdy_file = f"{bdy_dir}/cmems_nrt_bc_V110/{date_str[:4]}/{date_str[4:6]}/{date_str[6:]}/00/bdy_hourly_2d_y{date_str[:4]}m{date_str[4:6]}d{date_str[6:]}.nc"
    ssh_rec_raw = f"{output_dir}/ssh_rec/ssh_rec.{date_fmt}.t0000.nc"
    ssh_rec_remap = f"{output_dir}/ssh_rec/ssh_rec_bdyremap.{date_fmt}.nc"
    check_out = f"{output_dir}/debug_boundary_strip.{date_fmt}.nc"

    if not os.path.exists(bdy_file):
        raise FileNotFoundError(f"Boundary file not found: {bdy_file}")
    if not os.path.exists(ssh_rec_raw):
        raise FileNotFoundError(f"EOF SSH not found: {ssh_rec_raw}")

    # === Remap EOF using same grid as boundary ===
    cmd = f"cdo -s -O -L -f nc4 -z zip2 setmisstonn -remapbil,{gridfile} {ssh_rec_raw} {ssh_rec_remap}"
    print(f"[INFO] Remapping SSH reconstruction to boundary grid...")
    run_cmd(cmd)

    # === Load datasets ===
    ds_bdy = xr.open_dataset(bdy_file)
    ds_rec = xr.open_dataset(ssh_rec_remap)

    sla_bdy = ds_bdy["sla"]  # shape: (time=24, y, x=1)
    ssh_rec = ds_rec["ssh"].squeeze()  # shape: (y, x=1)

    # === Check compatibility ===
    if sla_bdy.shape[1:] != ssh_rec.shape:
        raise ValueError(f"Shape mismatch: boundary={sla_bdy.shape}, reconstruction={ssh_rec.shape}")

    # === Compute mean difference over time and lat
    sla_mean = sla_bdy.mean(dim="time")
    delta = (ssh_rec - sla_mean).mean().item()
    print(f"[INFO] Computed boundary delta (ssh_rec - mean(sla_bdy)): {delta:.5f} m")

    # === Save debug NetCDF
    debug_ds = xr.Dataset(
        data_vars=dict(
            sla_bdy=sla_bdy,
            ssh_rec=ssh_rec
        ),
        attrs=dict(description="Boundary SLA vs regridded SSH reconstruction")
    )

    debug_ds.to_netcdf(check_out, format="NETCDF4_CLASSIC")
    print(f"[OK] Debug boundary slice written to: {check_out}")

    return delta
