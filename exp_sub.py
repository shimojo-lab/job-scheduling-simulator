import subprocess
import os

exp = 7

TEMPLATE = """#!/bin/bash

#!/bin/bash

#PBS -q SQUID-H
#PBS --group=G15384
#PBS -l elapstim_req=12:00:00,cpunum_job=76

cd $PBS_O_WORKDIR
module load BasePy/2023
module --force switch python3/3.6 python3/3.8
export NUMEXPR_MAX_THREADS=76
source /sqfs/work/G15384/u6b815/job-scheduling-simulator/.venv/bin/activate
python3 exp4.py {exp} {method}

"""

for i in range(4):
    job_script = TEMPLATE.format(method=i, exp=exp)
    file = f"exp{exp}-{i}.sh"
    with open(file, "w") as f:
        f.write(job_script)
    subprocess.run(["qsub", file])
    os.remove(file)