import numpy as np
from enum import Enum
from modules.job import Job


class NodeState(Enum):
    IDLE = "idle"
    RUNNING = "running"
    COMPLETE = "complete"


class Resource:
    def __init__(self, node_size, timestep_seconds) -> None:
        self.nodes = [Node(timestep_seconds=timestep_seconds) for _ in range(node_size)]

    def print_nodes(self):
        print("*** Nodes ***")
        for i, node in enumerate(self.nodes):
            job_index = node.job.job_index if node.job else None
            print(
                f"Node {i}: {job_index} - {node.remaining_time} - {node.scheduled_remaining_timestep} - {node.state}"
            )

    def proceed_timestep(self):
        for node in self.nodes:
            node.proceed_timestep()

    def get_completed_jobs(self):
        completed_job_set = set()
        for node in self.nodes:
            if node.state == NodeState.COMPLETE:
                completed_job_set.add(node.job)
                node.state = NodeState.IDLE
        return list(completed_job_set)

    def get_running_jobs(self):
        running_job_set = set()
        for node in self.nodes:
            if node.state == NodeState.RUNNING:
                running_job_set.add(node.job)
        return list(running_job_set)

    def is_running(self):
        running = False
        for node in self.nodes:
            if node.state == NodeState.RUNNING:
                running = True
                break
        return running


class Node:
    def __init__(self, timestep_seconds):
        self.job: Job | None = None
        self.remaining_time = 0
        self.scheduled_remaining_timestep = 0
        self.timestep_seconds = timestep_seconds
        self.state: NodeState = NodeState.IDLE

    def proceed_timestep(self):
        if self.state == NodeState.RUNNING:
            self.remaining_time -= self.timestep_seconds
            if self.scheduled_remaining_timestep > 0:
                self.scheduled_remaining_timestep -= 1
            if self.remaining_time <= 0:
                self.state = NodeState.COMPLETE

    def allocate_job(self, job: Job):
        self.job = job
        self.remaining_time = job.real_time
        self.scheduled_remaining_timestep = np.ceil(
            job.pred_time / self.timestep_seconds
        ).astype(int)
        self.state = NodeState.RUNNING
