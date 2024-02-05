import pandas as pd

from modules.job import Workload
from modules.job_queue import JobQueue
from modules.resource import Resource
from modules.schedule import Schedule

DEBUG = True
NODE_SIZE = 10
SCHEDULE_TIMESTEP_WINDOW = 40
BACKFILL_TIMESTEP_WINDOW = 30
WATCH_JOB_SIZE = 5
TIMESTEP_SECONDS = 60

data = pd.read_parquet("data/jobs-previous-method.parquet")
if DEBUG:
    data = data[1000:1010]
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

# Create initial schedule
schedule.create_initial_schedule(job_queue, resource, workload.jobs)
schedule.print_schedule()

while not job_queue.is_empty() or resource.is_running():
    schedule.proceed_timestep(job_queue, resource, workload.jobs)
    schedule.print_schedule()
    # workload.print_jobs()
    resource.print_nodes()

workload.print_jobs()
