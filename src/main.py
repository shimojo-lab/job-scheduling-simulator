from typing import List
from models import Job, JobScheduler
from utils import formatted_time

SYSTEM_NODE_AMOUNT = 1400
FILENAME = "202107-all_users-pred_rfall"


# ジョブデータを読み込む
jobs: List[Job] = []
with open(f"runtime_estimated_data/{FILENAME}.csv") as f:
    for line in f:
        # skip header
        if line.startswith("id"):
            continue
        id, node_amount, req_runtime, act_runtime, pred_runtime = map(
            int, line.split(",")
        )
        jobs.append(Job(id, node_amount, req_runtime, act_runtime, pred_runtime))

# print jobs
print("\n---jobs input---")
print("id\t node\t req_time\t act_time\t pred_time")
for job in jobs:
    print(
        "Job %d:\t %d\t %s\t %s\t %s\t"
        % (
            job.id,
            job.node_amount,
            formatted_time(job.req_runtime),
            formatted_time(job.act_runtime),
            formatted_time(job.pred_runtime),
        )
    )


# ジョブスケジューラを作成する
scheduler = JobScheduler(SYSTEM_NODE_AMOUNT, jobs)

scheduler.run()
scheduler.show_jobs_start_time()
