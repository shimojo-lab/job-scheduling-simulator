import numpy as np
from tqdm import tqdm
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parents[1]))
from modules.job import Workload
from modules.job_queue import JobQueue
from modules.resource import Resource
from modules.schedule import Schedule


class Simulator:
    def __init__(
        self,
        data,
        NODE_SIZE,
        SCHEDULE_TIMESTEP_WINDOW,
        BACKFILL_TIMESTEP_WINDOW,
        WATCH_JOB_SIZE,
        TIMESTEP_SECONDS,
    ):
        self.NODE_SIZE = NODE_SIZE
        self.SCHEDULE_TIMESTEP_WINDOW = SCHEDULE_TIMESTEP_WINDOW
        self.BACKFILL_TIMESTEP_WINDOW = BACKFILL_TIMESTEP_WINDOW
        self.WATCH_JOB_SIZE = WATCH_JOB_SIZE
        self.TIMESTEP_SECONDS = TIMESTEP_SECONDS
        self.workload = Workload(data, TIMESTEP_SECONDS)
        self.job_queue = JobQueue(self.workload.jobs)
        self.schedule = Schedule(
            NODE_SIZE,
            SCHEDULE_TIMESTEP_WINDOW,
            BACKFILL_TIMESTEP_WINDOW,
            TIMESTEP_SECONDS,
            WATCH_JOB_SIZE,
        )
        self.resource = Resource(node_size=NODE_SIZE, timestep_seconds=TIMESTEP_SECONDS)

    def run(self, exp, method):
        with open(f'exp{exp}-method{method}-progress.txt', 'w') as f:
            pbar = tqdm(total=len(self.workload.jobs), file=f)

            # Create initial schedule
            allocated_job_count = self.schedule.create_initial_schedule(
                self.job_queue, self.resource, self.workload.jobs
            )
            pbar.update(allocated_job_count)
            f.flush()

            last_showed_progress = 0
            while not self.job_queue.is_empty() or self.resource.is_running():
                allocated_job_count = self.schedule.proceed_timestep(
                    self.job_queue, self.resource, self.workload.jobs
                )
                pbar.update(allocated_job_count)
                progress = np.ceil(pbar.n / pbar.total * 100)
                if progress - last_showed_progress >= 1:
                    f.flush()
                    last_showed_progress = progress

            pbar.close()

        # Show statics
        total_time = self.schedule.total_time()
        total_jobs = len(self.workload.jobs)
        avg_wall_time = self.workload.get_avg_wall_time()
        backfill_ratio = self.workload.get_backfill_ratio()
        job_throughput = total_jobs / (total_time / 3600)

        print(f"Total time: {total_time/3600:.2f} hours")
        print(f"Average wall time: {avg_wall_time/3600:.2f} hours")
        print(f"Job throughput: {job_throughput:.2f} jobs/hour")
        print(f"Backfill ratio: {backfill_ratio:.2f}")
        print("\n")
