import os
import subprocess
import sys
import shutil
import glob
from datetime import datetime, timedelta


class NemoModelRunner:
    def __init__(self, yystart, mmstart, ddstart,ndays):
        self.yystart = yystart
        self.mmstart = mmstart
        self.ddstart = ddstart
        self.ndays   = ndays 

        # You may adapt these paths
        self.maindir = "/ec/res4/scratch/eeim/nemo/"
#        self.setupdir = "/ec/res4/hpcperm/eeim/nemo_ecmwf/setup_N421/"
#        self.workdir = f"{self.maindir}NEMO42_EST_0.5nm_op_{yystart}{mmstart}{ddstart}/"
#        self.runid = "EST05nm_op_rerun11"
        self.setupdir = "/ec/res4/hpcperm/eeim/nemo_ecmwf/setup_N5/"
        self.workdir = f"{self.maindir}NEMO5_EST_0.5nm_op_{yystart}{mmstart}{ddstart}/"
        self.runid = "EST05nm_op_rerun20"
        self.scrdir = "/ec/res4/hpcperm/eeim/nemo_ecmwf/main_scripts/"
        self.meteodir = "/ec/res4/hpcperm/eeim/nemo_ecmwf/forcing/meteo/"
        self.runoffdir = "/ec/res4/hpcperm/eeim/nemo_ecmwf/forcing/runoff/"
        self.bdydir = "/ec/res4/hpcperm/eeim/nemo_ecmwf/forcing/boundary/"

        self.prepare_workdir()

    def run(self, cmd):
        print(f"Running: {cmd}")
        result = subprocess.run(cmd, shell=True)
        if result.returncode != 0:
            print(f"ERROR running command: {cmd}")
            sys.exit(1)

    def prepare_workdir(self):
        os.makedirs(self.workdir, exist_ok=True)
        os.chdir(self.workdir)

        # Copy namelists, xmls, binaries
        self.run(f"cp {self.setupdir}/namelist_* {self.workdir}/")
        self.run(f"cp {self.setupdir}/*.xml {self.workdir}/")
        self.run(f"cp {self.setupdir}/do_* {self.workdir}/")
        self.run(f"cp {self.setupdir}/do_bdy2d_conv.py {self.workdir}/")

        self.run(f"ln -sf {self.setupdir}/coordinates.bdy.nc {self.workdir}/")
        self.run(f"ln -sf {self.setupdir}/bfr_roughness.nc {self.workdir}/")
        self.run(f"cp {self.setupdir}/run_nemo {self.workdir}/")
        self.run(f"ln -sf {self.setupdir}/domain_cfg_EST_0.5nm_V110_fix.nc {self.workdir}/domain_cfg_EST_0.5nm_V110.nc")
        self.run(f"ln -sf {self.setupdir}/nemo.exe {self.workdir}/nemo.exe")
        self.run(f"ln -sf {self.setupdir}/xios_server.exe {self.workdir}/xios_server.exe")

        # Create subfolders
        os.makedirs(f"{self.workdir}/initialstate", exist_ok=True)
        os.makedirs(f"{self.workdir}/restarts", exist_ok=True)
        os.makedirs(f"{self.workdir}/runoff_seas", exist_ok=True)
        os.makedirs(f"{self.workdir}/bc_V110", exist_ok=True)
        os.makedirs(f"{self.workdir}/forcing_ecmwf", exist_ok=True)
        os.makedirs(f"{self.workdir}/output/{self.runid}/{self.yystart}/{self.mmstart}/{self.ddstart}", exist_ok=True)

    def configure_run(self):
        # Cold or hot start logic simplified:
        rdate = "20211101"
        current_date = datetime.now().strftime("%Y%m%d")

        start_date = datetime.strptime(f"{self.yystart}{self.mmstart}{self.ddstart}", "%Y%m%d")
        self.rlen_hours = self.ndays * 24
        if f"{self.yystart}{self.mmstart}{self.ddstart}" == rdate:
            self.ln_rstart = ".false."
            self.ln_tsd_init = ".true."
            self.ln_iceini = ".true."
            #self.rlen_hours = 24
            self.run(f"ln -sf {self.setupdir}/../forcing/initial/initial_run_t{rdate}.nc {self.workdir}/initial_run.nc")
        elif f"{self.yystart}{self.mmstart}{self.ddstart}" != current_date:
            self.ln_rstart = ".true."
            self.ln_tsd_init = ".false."
            self.ln_iceini = ".false."
            #self.rlen_hours = 24
        else:
            self.ln_rstart = ".true."
            self.ln_tsd_init = ".false."
            self.ln_iceini = ".false."
            self.rlen_hours = 120
    
        self.start_date = start_date
        self.stop_date = start_date + timedelta(hours=self.rlen_hours)
        self.n_days = (self.stop_date - self.start_date).days
        self.n_sec = (self.stop_date - self.start_date).total_seconds()

        self.rn_rdt = 240
        self.nn_itend = int(self.n_sec / self.rn_rdt)
        self.stock1 = int(24 * 3600 / self.rn_rdt)
        self.stock2 = int(2 * 24 * 3600 / self.rn_rdt)

        with open("stock1_num.dat", "w") as f:
            f.write(str(self.stock1) + "\n")
        with open("stock2_num.dat", "w") as f:
            f.write(str(self.stock2) + "\n")

    def generate_namelists(self):
        # Generate namelist_ref
        with open(f"{self.setupdir}/namelist_ref_template_V110_op") as fin:
            content = fin.read()
        content = content.replace("_runid_", self.runid)\
                         .replace("_nn_itend_", str(self.nn_itend))\
                         .replace("_stock1_", str(self.stock1))\
                         .replace("_stock2_", str(self.stock2))\
                         .replace("_ln_rstart_", self.ln_rstart)\
                         .replace("_nn_date0_", f"{self.yystart}{self.mmstart}{self.ddstart}")\
                         .replace("_rn_rdt_", str(self.rn_rdt))\
                         .replace("_ln_tsd_init_", self.ln_tsd_init)

        with open("namelist_ref", "w") as fout:
            fout.write(content)

        # Generate namelist_ice_ref
        with open(f"{self.setupdir}/namelist_ice_ref_template_mm") as fin:
            ice_content = fin.read()
        ice_content = ice_content.replace("_ln_iceini_", self.ln_iceini)
        with open("namelist_ice_ref", "w") as fout:
            fout.write(ice_content)
    def link_restart(self):
        print("\n===== Linking Restart Files =====")

        # Determine previous day
        restart_date = self.start_date - timedelta(days=1)
        yyrest, mmrest, ddrest = restart_date.strftime("%Y"), restart_date.strftime("%m"), restart_date.strftime("%d")
        base_restart_dir = "/ec/res4/scratch/eeim/nemo/"
        rdir = f"{base_restart_dir}NEMO42_EST_0.5nm_op_{yyrest}{mmrest}{ddrest}/"
        print(f"Looking for restart in: {rdir}")

        if not os.path.exists(rdir):
            print(f"ERROR: Restart directory not found: {rdir}")
            sys.exit(1)

        # Detect restart step number
        stock1_file = os.path.join(rdir, "stock1_num.dat")
        if os.path.exists(stock1_file):
            with open(stock1_file) as f:
                num = int(f.read().strip())
            print(f"Read restart timestep from stock1_num.dat: {num}")
        else:
            num = self.stock1
            print(f"WARNING: stock1_num.dat not found, using default restart timestep: {num}")

        restnn = f"{num:08d}"
        reststr = f"{self.runid}_{restnn}"
        nd = 256  # number of subdomains (adapt if needed)

        print(f"Restart string: {reststr}")
        print(f"Linking {nd} domain restart files...")

        restart_src_dir = os.path.join(rdir, "restarts")
        restart_dst_dir = os.path.join(self.workdir, "initialstate")

        if not os.path.exists(restart_dst_dir):
            os.makedirs(restart_dst_dir)

        # Clean existing initialstate links
        self.run(f"rm -f {restart_dst_dir}/restart_in_*.nc")
        self.run(f"rm -f {restart_dst_dir}/restart_ice_in_*.nc")

        for d in range(nd):
            dd = f"{d:04d}"
            ocean_src = f"{restart_src_dir}/{reststr}_restart_out_{dd}.nc"
            ice_src = f"{restart_src_dir}/{reststr}_restart_ice_out_{dd}.nc"

            ocean_dst = f"{restart_dst_dir}/restart_in_{dd}.nc"
            ice_dst = f"{restart_dst_dir}/restart_ice_in_{dd}.nc"

            if os.path.exists(ocean_src):
                print(f"Linking ocean restart: {ocean_src} -> {ocean_dst}")
                self.run(f"ln -sf {ocean_src} {ocean_dst}")
            else:
                print(f"WARNING: Missing ocean restart: {ocean_src}")

            if os.path.exists(ice_src):
                print(f"Linking ice restart: {ice_src} -> {ice_dst}")
                self.run(f"ln -sf {ice_src} {ice_dst}")
            else:
                print(f"WARNING: Missing ice restart: {ice_src}")
            
#import glob
#    def link_meteo(self):
#        self.run(f"rm {self.workdir}/forcing_ecmwf/FORCE_*")
#        self.run(f"ln -sf {self.meteodir}/meteo_nemo_ecmwf_BAL/weights_meteo* {self.workdir}/forcing_ecmwf/")
#        meteo_path = f"{self.meteodir}/meteo_nemo_ecmwf_BAL/{self.yystart}/{self.mmstart}/{self.ddstart}/00/"
#        self.run(f"ln -sf {meteo_path}/FORCE_*{self.workdir}/ forcing_ecmwf/")
    def link_meteo(self):
        print("\n===== Linking Meteo Forcing =====")

        self.run(f"rm -f {self.workdir}/forcing_ecmwf/FORCE_*")
        self.run(f"ln -sf {self.meteodir}/meteo_nemo_ecmwf_BAL/weights_meteo* {self.workdir}/forcing_ecmwf/")

        meteo_path = f"{self.meteodir}/meteo_nemo_ecmwf_BAL/{self.yystart}/{self.mmstart}/{self.ddstart}/00/"
        print(f"Looking for meteo forcing files in: {meteo_path}")

        if not os.path.exists(meteo_path):
            print(f"ERROR: Meteo forcing directory does not exist: {meteo_path}")
            sys.exit(1)

        forcing_files = glob.glob(f"{meteo_path}/FORCE_*")
        if not forcing_files:
            print(f"WARNING: No meteo forcing files found in {meteo_path}")
        else:
            for file in forcing_files:
                filename = os.path.basename(file)
                print(f"Linking {file} -> {self.workdir}/forcing_ecmwf/{filename}")
                self.run(f"ln -sf {file} {self.workdir}/forcing_ecmwf/{filename}")

    def link_runoff(self):
        print("\n===== Linking Runoff Forcing =====")

        # Remove existing runoff files
        self.run(f"rm -f {self.workdir}/runoff_seas/river_data_*")

        rqdir = f"{self.runoffdir}/runoff_q_seasonal"
        rtdir = f"{self.runoffdir}/runoff_t_atmt2"

        # Link static seasonal runoff salinity file
        river_static_src = f"{self.runoffdir}/river_data_s_c0.1.nc"
        river_static_dst = f"{self.workdir}/runoff_seas/river_data_s.nc"

        print(f"Linking static river_data_s_c0.1.nc: {river_static_src} -> {river_static_dst}")
        if not os.path.exists(river_static_src):
            print(f"ERROR: Static runoff file not found: {river_static_src}")
            sys.exit(1)
        self.run(f"ln -sf {river_static_src} {river_static_dst}")

        # Now link dynamic daily runoff files
        for day_offset in range(self.n_days + 1):
            date = self.start_date + timedelta(days=day_offset)
            yy, mm, dd = date.strftime("%Y"), date.strftime("%m"), date.strftime("%d")

            q_src = f"{rqdir}/river_data_y2015m{mm}d{dd}.nc"
            q_dst = f"{self.workdir}/runoff_seas/river_data_y{yy}m{mm}d{dd}.nc"
            print(f"Linking runoff discharge: {q_src} -> {q_dst}")

            if not os.path.exists(q_src):
                print(f"WARNING: Runoff discharge file missing: {q_src}")
            else:
                self.run(f"ln -sf {q_src} {q_dst}")

            t_src = f"{rtdir}/river_data_t_y{self.yystart}m{self.mmstart}d{self.ddstart}.nc"
            t_dst = f"{self.workdir}/runoff_seas/river_data_t_y{yy}m{mm}d{dd}.nc"
            print(f"Linking runoff temperature: {t_src} -> {t_dst}")

            if not os.path.exists(t_src):
                print(f"WARNING: Runoff temperature file missing: {t_src}")
            else:
                self.run(f"ln -sf {t_src} {t_dst}")

    def link_boundary(self):
        print("\n===== Linking Boundary Forcing =====")

        self.run(f"rm -f {self.workdir}/bc_V110/bdy*")

        bdydir_run = f"{self.bdydir}/cmems_nrt_bc_V110/{self.yystart}/{self.mmstart}/{self.ddstart}/00/"
        print(f"Boundary directory: {bdydir_run}")

        if not os.path.exists(bdydir_run):
            print(f"ERROR: Boundary directory does not exist: {bdydir_run}")
            sys.exit(1)

        # Link 2D boundary files
        bdy2d_files = glob.glob(f"{bdydir_run}/bdy_hourly_2d_*")
        if not bdy2d_files:
            print(f"WARNING: No 2D boundary files found in {bdydir_run}")
        else:
            for file in bdy2d_files:
                filename = os.path.basename(file)
                target = f"{self.workdir}/bc_V110/{filename}"
                print(f"Linking 2D boundary: {file} -> {target}")
                self.run(f"ln -sf {file} {target}")

        # Link 3D boundary files
        bdy3d_files = glob.glob(f"{bdydir_run}/bdy_hourly_3d_*")
        if not bdy3d_files:
            print(f"WARNING: No 3D boundary files found in {bdydir_run}")
        else:
            for file in bdy3d_files:
                filename = os.path.basename(file)
                target = f"{self.workdir}/bc_V110/{filename}"
                print(f"Linking 3D boundary: {file} -> {target}")
                self.run(f"ln -sf {file} {target}")


    def launch_model(self):
        print("\n===== Launching NEMO Model =====")

        # Cleanup before model run
        cleanup_files = [
            "model_last_start", "model_last_end",
            "log.stdout", "log.stderr",
            "ocean.output"
        ]

        for file in cleanup_files:
            file_path = os.path.join(self.workdir, file)
            if os.path.exists(file_path):
                print(f"Removing old file: {file_path}")
                os.remove(file_path)

        # Write model_last_start timestamp
        now_timestamp = int(datetime.now().timestamp())
        with open(os.path.join(self.workdir, "model_last_start"), "w") as f:
            f.write(str(now_timestamp) + "\n")
        print(f"Model start time written to model_last_start: {now_timestamp}")

        # Submit the actual NEMO run via sbatch
        run_command = f"sbatch -W --export=ALL,model_run_dir={self.workdir} run_nemo"
        print(f"Submitting NEMO job with command: {run_command}")

        result = subprocess.run(run_command, shell=True)
        if result.returncode != 0:
            print(f"ERROR: sbatch job submission failed!")
            sys.exit(1)
        else:
            print("NEMO job finished successfully.")


    import shutil
    def copy_output_files_with_next_day_stamp(runid, rundir, yyyy2, mm2, dd2, yyyye, mme, dde):
        print("\n===== Copying Output Files with Updated Timestamp =====")

        filenames = [
            f"{runid}_1h_stuvw_{yyyy2}{mm2}{dd2}-{yyyy2}{mm2}{dd2}.nc",
            f"{runid}_1d_ice_grid_T_{yyyy2}{mm2}{dd2}-{yyyy2}{mm2}{dd2}.nc",
            f"{runid}_1h_SURF_grid_T_{yyyy2}{mm2}{dd2}-{yyyy2}{mm2}{dd2}.nc",
            f"{runid}_1h_SURF_grid_U_{yyyy2}{mm2}{dd2}-{yyyy2}{mm2}{dd2}.nc",
            f"{runid}_1h_SURF_grid_V_{yyyy2}{mm2}{dd2}-{yyyy2}{mm2}{dd2}.nc",
        ]

        for fname in filenames:
            src_path = os.path.join(rundir, fname)
            new_end_str = f"{yyyye}{mme}{dde}"
            dst_fname = fname.replace(f"{yyyy2}{mm2}{dd2}-{yyyy2}{mm2}{dd2}", f"{yyyy2}{mm2}{dd2}-{new_end_str}")
            dst_path = os.path.join(rundir, dst_fname)

            if os.path.exists(src_path):
                print(f"Copying {src_path} → {dst_path}")
                shutil.copy2(src_path, dst_path)
            else:
                print(f"WARNING: Source file not found: {src_path}")


    def upload_outputs(self, lt0=[0, 1, 2, 3, 4, 5]):
        print("\n===== Uploading Model Outputs via rsync =====")

        USER = "ilja.maljutenko"
        HOST = "atlas.msi.ttu.ee"
        odir = "/mnt/archive/ECMWF_op/"
        rsyncop = "-avz --inplace --checksum --ignore-times"
        rundir = self.workdir  # already defined in the class
        runid = self.runid

        for n in lt0:
            # Date range for filename
            start_dt = self.start_date + timedelta(days=n)
            end_dt = self.start_date + timedelta(days=n + 1)

            yyyy2, mm2, dd2 = start_dt.strftime("%Y"), start_dt.strftime("%m"), start_dt.strftime("%d")
            yyyye, mme, dde = end_dt.strftime("%Y"), end_dt.strftime("%m"), end_dt.strftime("%d")
            # Files to prepare
            files_to_copy_and_upload = [
                ("1h_stuvw", "NEMO/rerun/{runid}_1h_stuvw/"),
                ("1d_ice_grid_T", "NEMO/rerun/{runid}_1d_ice/"),
                ("1h_SURF_grid_T", "NEMO/rerun/{runid}_1h_surf/"),
                ("1h_SURF_grid_U", "NEMO/rerun/{runid}_1h_surf/"),
                ("1h_SURF_grid_V", "NEMO/rerun/{runid}_1h_surf/"),
            ]
#            file_pairs = [
#                (f"{runid}_1h_stuvw_{yyyy2}{mm2}{dd2}-{yyyye}{mme}{dde}.nc", f"{odir}/NEMO/rerun/{runid}_1h_stuvw/"),
#                (f"{runid}_1d_ice_grid_T_{yyyy2}{mm2}{dd2}-{yyyye}{mme}{dde}.nc", f"{odir}/NEMO/rerun/{runid}_1d_ice/"),
#                (f"{runid}_1h_SURF_grid_T_{yyyy2}{mm2}{dd2}-{yyyye}{mme}{dde}.nc", f"{odir}/NEMO/rerun/{runid}_1h_surf/"),
#                (f"{runid}_1h_SURF_grid_U_{yyyy2}{mm2}{dd2}-{yyyye}{mme}{dde}.nc", f"{odir}/NEMO/rerun/{runid}_1h_surf/"),
#                (f"{runid}_1h_SURF_grid_V_{yyyy2}{mm2}{dd2}-{yyyye}{mme}{dde}.nc", f"{odir}/NEMO/rerun/{runid}_1h_surf/"),
#            ]
            for file_prefix, remote_template in files_to_copy_and_upload:
                same_day = f"{runid}_{file_prefix}_{yyyy2}{mm2}{dd2}-{yyyy2}{mm2}{dd2}.nc"
                next_day = f"{runid}_{file_prefix}_{yyyy2}{mm2}{dd2}-{yyyye}{mme}{dde}.nc"

                src_path = os.path.join(rundir, same_day)
                dst_path = os.path.join(rundir, next_day)

                if os.path.exists(src_path):
                    print(f"Copying for upload: {src_path} → {dst_path}")
                    shutil.copy2(src_path, dst_path)
                else:
                    print(f"WARNING: Expected source file for copying not found: {src_path}")
                    continue  # Skip rsync if copy failed

                # Prepare remote path
                remote_path = remote_template.format(runid=runid)
                rsync_cmd = (
                    f"rsync {rsyncop} "
                    f"--rsh='ssh -p2222' "
                    f"--rsync-path=\"mkdir -p {odir}{remote_path} && rsync\" "
                    f"{dst_path} {USER}@{HOST}:{odir}{remote_path}"
                )

                print(f"Uploading: {dst_path} → {odir}{remote_path}")
                result = subprocess.run(rsync_cmd, shell=True)
                if result.returncode != 0:
                    print(f"ERROR: rsync failed for {dst_path}")
#            for fname, remotepath in file_pairs:
#                fpath = os.path.join(rundir, fname)
#                if not os.path.exists(fpath):
#                    print(f"WARNING: File not found: {fpath} — skipping upload")
#                    continue
#
#                rsync_cmd = (
#                    f"rsync {rsyncop} "
#                    f"--rsh='ssh -p2222' "
#                    f"--rsync-path=\"mkdir -p {remotepath} && rsync\" "
#                    f"{fpath} {USER}@{HOST}:{remotepath}"
#                )
#
#                print(f"Uploading {fname} -> {remotepath}")
#                result = subprocess.run(rsync_cmd, shell=True)
#                if result.returncode != 0:
#                    print(f"ERROR uploading {fname}")


    def full_run(self):
        self.configure_run()
        self.prepare_workdir()         
        self.generate_namelists()
        self.link_restart()
        self.link_meteo()
        self.link_runoff()
        self.link_boundary()
        self.launch_model()
        self.upload_outputs(lt0=[0])
