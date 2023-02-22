from models import Job, JobScheduler
from utils import formatted_time

NODE_COUNT = 10


# ジョブデータを読み込む
jobs = []
with open("job_data.csv") as f:
    for line in f:
        # skip header
        if line.startswith("id"):
            continue
        id, node, req_sec, act_sec = map(int, line.split(","))
        jobs.append(Job(id, node, req_sec, act_sec))

# print jobs
print("\n---jobs input---")
for job in jobs:
    print("Job %d: %d, %s" % (job.id, job.node, formatted_time(job.act_sec)))


# ジョブスケジューラを作成する
scheduler = JobScheduler(NODE_COUNT, jobs)

scheduler.run()
scheduler.show_jobs_start_time()
