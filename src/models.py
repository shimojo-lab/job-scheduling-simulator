from typing import List
from enum import Enum

from utils import formatted_time


class Job:
    def __init__(self, id, node, req_sec, act_sec, pred_sec):
        self.id = id
        self.node = node
        self.req_sec = req_sec
        self.act_sec = act_sec
        self.pred_sec = pred_sec
        self.start_time_act = 0
        self.start_time_sched = 0
        self.start_time_pred = 0


class SchedTimeType(Enum):
    ACT = 0
    SCHED = 1
    PRED = 2


class Node:
    def __init__(self):
        self.avail = True
        self.time_to_be_avail = 0


class JobScheduler:
    def __init__(self, nodes_count, job_data):
        self.job_data: List[Job] = job_data
        self.nodes = [Node() for _ in range(nodes_count)]
        self.time = 0

    def reset(self):
        self.time = 0
        self.nodes = [Node() for _ in range(len(self.nodes))]

    def run(self):
        # 実際のジョブの開始時間を計算
        self.schedule(SchedTimeType.ACT)
        # スケジュールされたジョブの開始時間を計算
        self.schedule(SchedTimeType.SCHED)
        # 予測されたジョブの開始時間を計算
        self.schedule(SchedTimeType.PRED)

    def schedule(self, sched_time_type: SchedTimeType):
        for job in self.job_data:
            while True:
                if self.check_nodes(job.node):
                    self.assign_job(job, sched_time_type)
                    break
                else:
                    self.time += 1
                    self.update_nodes()
        self.reset()

    def check_nodes(self, nodes_needed):
        avail_nodes_count = 0
        for node in self.nodes:
            if node.avail:
                avail_nodes_count += 1
        return avail_nodes_count >= nodes_needed

    def assign_job(self, job: Job, sched_time: SchedTimeType):
        node_indices = self.get_avail_node_index(job.node)
        for node_index in node_indices:
            self.nodes[node_index].avail = False
            if sched_time == SchedTimeType.ACT:
                lapse_sec = job.act_sec
            elif sched_time == SchedTimeType.SCHED:
                lapse_sec = job.req_sec
            elif sched_time == SchedTimeType.PRED:
                lapse_sec = job.pred_sec
            self.nodes[node_index].time_to_be_avail = self.time + lapse_sec
        if sched_time == SchedTimeType.ACT:
            job.start_time_act = self.time
        elif sched_time == SchedTimeType.SCHED:
            job.start_time_sched = self.time
        elif sched_time == SchedTimeType.PRED:
            job.start_time_pred = self.time

    # 利用可能なノードのインデックスを取得する
    def get_avail_node_index(self, nodes_needed):
        avail_nodes = []
        for i, node in enumerate(self.nodes):
            if node.avail:
                avail_nodes.append(i)
        return avail_nodes[:nodes_needed]

    def update_nodes(self):
        for node in self.nodes:
            if node.time_to_be_avail == self.time:
                node.avail = True

    def show_jobs_start_time(self):
        print("\n---jobs start time (act)---")
        for job in self.job_data:
            print("Job %d: %s" % (job.id, formatted_time(job.start_time_act)))

        print("\n---jobs start time (sched)---")
        for job in self.job_data:
            print("Job %d: %s" % (job.id, formatted_time(job.start_time_sched)))

        print("\n---jobs start time (pred)---")
        for job in self.job_data:
            print("Job %d: %s" % (job.id, formatted_time(job.start_time_pred)))

        # Compute mean squared error of sched, pred for act
        mse_sched = 0
        mse_pred = 0
        for job in self.job_data:
            mse_sched += (job.start_time_sched - job.start_time_act) ** 2
            mse_pred += (job.start_time_pred - job.start_time_act) ** 2
        mse_sched /= len(self.job_data)
        mse_pred /= len(self.job_data)
        print("\n---MSE---")
        print("sched: %f" % mse_sched)
        print("pred: %f" % mse_pred)
        