import pandas as pd
import numpy as np
from typing import List


class Workload:
    def __init__(self, data: pd.DataFrame, timestep_seconds: int):
        self.jobs = self.from_dataframe(data, timestep_seconds)

    def print_jobs(self):
        print("*** Jobs ***")
        print(
            "job_index\tlog_id\tpred_time\treal_time\tnode_size\tqueued_timestep\tscheduled_timestep\tis_backfilled\tallocated_node_indecies\toccupied_range"
        )
        for job in self.jobs:
            print(job)

    def from_dataframe(self, data, timestep_seconds) -> List["Job"]:
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
                    timestep_length=np.ceil(pred_time / timestep_seconds).astype(int),
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
        timestep_length: int,
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
        self.allocated_node_indecies = []
        self.timestep_length = timestep_length
        self.occupied_range = [0, 0]

    def __repr__(self):
        return f"{self.job_index}\t{self.log_id}\t{self.pred_time}\t\t{self.real_time}\t\t{self.node_size}\t\t{self.queued_timestep}\t\t{self.scheduled_timestep}\t\t\t{self.is_backfilled}\t\t{self.allocated_node_indecies}\t{self.occupied_range}"

    def set_queued_timestep(self, timestep: int):
        if self.queued_timestep is None:
            self.queued_timestep = timestep
