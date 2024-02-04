import pandas as pd
from typing import List


class Workload:
    def __init__(self, data: pd.DataFrame):
        self.jobs = self.from_dataframe(data)

    def print_jobs(self):
        print("*** Jobs ***")
        for job in self.jobs:
            print(job)

    def from_dataframe(self, data) -> List["Job"]:
        jobs = []
        for idx, (index, row) in enumerate(data.iterrows()):
            log_id = row["log_id"]
            real_time = int(row["y_true"])  # Ensure real_time is an integer
            pred_time = int(
                round(row["y_pred"])
            )  # Round and ensure pred_time is an integer
            node_size = int(row["ehost_num"])
            jobs.append(
                Job(
                    job_index=idx,
                    log_id=log_id,
                    pred_time=pred_time,
                    real_time=real_time,
                    node_size=node_size,
                )
            )
        return jobs


class Job:
    def __init__(
        self,
        job_index: int,
        log_id: int,
        pred_time: int,
        real_time: int,
        node_size: int,
    ):
        self.job_index = job_index
        self.log_id = log_id
        self.pred_time = pred_time
        self.real_time = real_time
        self.node_size = node_size
        self.queued_timestep: int | None = None
        self.scheduled_timestep: int | None = None
        self.is_backfilled = False
        self.allocated_nodes = []

    def __repr__(self):
        return f"Job {self.job_index}: {self.log_id} - {self.pred_time} - {self.real_time} - {self.node_size} - {self.scheduled_timestep} - {self.allocated_nodes}"
