run_nemo_ecmwf_workflow.py
├── Parse args (--date, --ndays)
├── Adjust date: date - 1 day
├── Set ENV: FORCINGDIR, CONFDIR
├── Coldstart branch (if date == 20231230)
│   ├── initialize_case(case_id=0)
│   │   └── init_case0_coldstart → download & remap CMEMS θ, S
│   └── generate_sla_increment + create_assim_background_files
├── Else (Operational)
│   └── generate_operational_ssh_increment
├── STEP 2: run_physical_boundary(date, ndays)
│   └── remap CMEMS hourly fields → boundary strips
├── STEP 3: generate_meteo_ecmwf(date, ndays)
│   └── fetch MARS, compute t2, u10, v10, lwr, swr, etc.
├── STEP 4: generate_runoff(date, ndays)
│   └── look back 4 days of FORCE → compute rotemp = max(t2 - 274.15, 0.1)
├── STEP 5: NemoModelRunner(...)
│   ├── prepare_workdir()
│   ├── configure_run()
│   ├── generate_namelists()
│   ├── link_restart() / coldstart
│   ├── link_meteo(), link_runoff(), link_boundary()  
│   └── launch_model() → sbatch run_nemo
TODO
├──  DEODE meteo prep
├──  Launch model 
├──  convert output
