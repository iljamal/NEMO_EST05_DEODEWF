┌────────────────────────────────────────────────────────────┐
│ Main script entrypoint:                                   │
│     python run_nemo_ecmwf_workflow.py --date YYYYMMDD     │
└────────────────────────────────────────────────────────────┘
              │
              ▼
     Parse --date and shift -1 day for spin-up
              │
              ▼
  Set ENV vars FORCINGDIR + CONFDIR + HPCPERM fallback
              │
              ▼
     ┌────────────────────────────────────────┐
     │ if date_str == "20231230" (coldstart): │
     └────────────────────────────────────────┘
              │
              ├──▶ `initialize_case(case_id=0)`
              │       └─ calls `init_case0_coldstart(...)`
              │             └─ uses CMEMS data to build
              │                initial 3D fields (thetao, so)
              │
              └──▶ SLA assimilation increment:
                      • `generate_sla_increment(...)`
                      • `create_assim_background_files(...)`

     ┌─────────────────────────────────────────────┐
     │ else: (operational / restart-based)         │
     └─────────────────────────────────────────────┘
              └──▶ `generate_operational_ssh_increment(...)`

              ▼
    ┌────────────────────────────────────────────┐
    │ STEP 2: Physical boundary forcing          │
    └────────────────────────────────────────────┘
              └──▶ `run_physical_boundary(date_str, ndays)`
                      ├─ Downloads CMEMS hourly data
                      ├─ Remaps to boundary grid
                      └─ Splits into daily 2D/3D files

              ▼
    ┌────────────────────────────────────────────┐
    │ STEP 3: ECMWF Meteo forcing                │
    └────────────────────────────────────────────┘
              └──▶ `generate_meteo_ecmwf(date_str, ndays)`
                      ├─ Fetch via MARS API
                      ├─ Process t2, u10, v10, swr, lwr, etc
                      └─ Outputs: FORCE_ecmwf_yYYYYmMMdDD.nc

              ▼
    ┌────────────────────────────────────────────┐
    │ STEP 4: Runoff forcing                     │
    └────────────────────────────────────────────┘
              └──▶ `generate_runoff(date_str, ndays)`
                      ├─ Looks back up to 4 days of meteo t2
                      ├─ Computes rotemp = max(t2 - 274.15, 0.1)
                      └─ Remaps to bathymetry grid

              ▼
    ┌────────────────────────────────────────────┐
    │ STEP 5: Run NEMO model                     │
    └────────────────────────────────────────────┘
              └──▶ `NemoModelRunner(...)`
                      ├─ `prepare_workdir()`
                      ├─ `configure_run()`
                      ├─ `generate_namelists()`
                      ├─ `link_restart()` or coldstart
                      ├─ `link_meteo()`
                      ├─ `link_runoff()`
                      ├─ `link_boundary()`
                      └─ `launch_model()`
                              └─ uses sbatch run_nemo

              ▼
    ┌────────────────────────────────────────────┐
    │ Optionally: SLA adjustment on boundaries   │
    └────────────────────────────────────────────┘
              └──▶ `cpandadjust_boundary()`
                      ├─ loads bdy_hourly_2d_*.nc
                      ├─ sets SLA[0] = 0.0
                      └─ links 3D boundary files

              ▼
    ┌────────────────────────────────────────────┐
    │ Post-run (optional): rsync, archive        │
    └────────────────────────────────────────────┘
