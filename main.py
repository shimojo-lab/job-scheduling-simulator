import pandas as pd
from tqdm import tqdm
from modules.job import Workload
from modules.job_queue import JobQueue
from modules.resource import Resource
from modules.schedule import Schedule

DEBUG = True

NODE_SIZE = 1520
SCHEDULE_TIMESTEP_WINDOW = 1440
BACKFILL_TIMESTEP_WINDOW = 720
WATCH_JOB_SIZE = 100
TIMESTEP_SECONDS = 60
SAMPLE_SIZE = 10000

data = pd.read_parquet("data/jobs-previous-method.parquet")
data = data.sample(SAMPLE_SIZE, random_state=1028)
if DEBUG:
    NODE_SIZE = 10
    SCHEDULE_TIMESTEP_WINDOW = 40
    BACKFILL_TIMESTEP_WINDOW = 30
    WATCH_JOB_SIZE = 5
    TIMESTEP_SECONDS = 60
    data = data[:1000]
    # data = pd.DataFrame(
    #     {
    #         "log_id": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    #         "y_true": [120, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110],
    #         "y_pred": [10, 180, 60, 300, 400, 500, 500, 400, 1800, 110, 120],
    #         "ehost_num": [5, 10, 10, 2, 1, 3, 4, 5, 6, 3, 3],
    #     }
    # )

# Initialize the job queue, schedule, and nodes
workload = Workload(data, TIMESTEP_SECONDS)
job_queue = JobQueue(workload.jobs)
schedule = Schedule(
    NODE_SIZE,
    SCHEDULE_TIMESTEP_WINDOW,
    BACKFILL_TIMESTEP_WINDOW,
    TIMESTEP_SECONDS,
    WATCH_JOB_SIZE,
)
resource = Resource(node_size=NODE_SIZE, timestep_seconds=TIMESTEP_SECONDS)

progress_bar = tqdm(total=len(workload.jobs))

# Create initial schedule
allocated_job_count = schedule.create_initial_schedule(
    job_queue, resource, workload.jobs
)
progress_bar.update(allocated_job_count)
# schedule.print_schedule()

while not job_queue.is_empty() or resource.is_running():
    allocated_job_count = schedule.proceed_timestep(job_queue, resource, workload.jobs)
    progress_bar.update(allocated_job_count)
    # schedule.print_schedule()
    # workload.print_jobs()
    # resource.print_nodes()

progress_bar.close()
# workload.print_jobs()

# Show statics
total_time = schedule.total_time()
total_jobs = len(workload.jobs)
avg_wall_time = workload.show_avg_wall_time()
job_throughput = total_jobs / (total_time / 3600)

print(f"Total time: {total_time/3600:.2f} hours")
print(f"Average wall time: {avg_wall_time/3600:.2f} hours")
print(f"Job throughput: {job_throughput:.2f} jobs/hour")
print("*** Params ***")
print(f"Node size: {NODE_SIZE}")
print(f"Timestep window: {SCHEDULE_TIMESTEP_WINDOW}")
print(f"Backfill window: {BACKFILL_TIMESTEP_WINDOW}")
print(f"Watch job size: {WATCH_JOB_SIZE}")
print(f"Timestep seconds: {TIMESTEP_SECONDS}")
print(f"Sample size: {SAMPLE_SIZE}")
