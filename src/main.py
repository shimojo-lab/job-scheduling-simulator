from typing import List
from models import Job, JobScheduler
from utils import formatted_time

NODE_COUNT = 10


# ジョブデータを読み込む
jobs:List[Job] = []
with open("job_data.csv") as f:
    for line in f:
        # skip header
        if line.startswith("id"):
            continue
        id, node, req_sec, act_sec, pred_sec = map(int, line.split(","))
        jobs.append(Job(id, node, req_sec, act_sec, pred_sec))

# print jobs
print("\n---jobs input---")
print("id\t node\t req_time\t act_time\t pred_time")
for job in jobs:
    print(
        "Job %d:\t %d\t %s\t %s\t %s\t"
        % (
            job.id,
            job.node,
            formatted_time(job.req_sec),
            formatted_time(job.act_sec),
            formatted_time(job.pred_sec),
        )
    )


# ジョブスケジューラを作成する
scheduler = JobScheduler(NODE_COUNT, jobs)

scheduler.run()
scheduler.show_jobs_start_time()
