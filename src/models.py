import numpy as np
from utils import formatted_time


class Job:
    def __init__(self, id, node, req_sec, act_sec):
        self.id = id
        self.node = node
        self.req_sec = req_sec
        self.act_sec = act_sec
        self.start_time = 0


class Node:
    def __init__(self):
        self.avail = True
        self.time_to_be_avail = 0


class JobScheduler:
    def __init__(self, nodes_count, job_data):
        self.job_data = job_data
        self.nodes = [Node() for _ in range(nodes_count)]
        self.time = 0

    def run(self):
        for job in self.job_data:
            while True:
                if self.check_nodes(job.node):
                    self.assign_job(job)
                    break
                else:
                    self.time += 1
                    self.update_nodes()

    def check_nodes(self, nodes_needed):
        avail_nodes_count = 0
        for node in self.nodes:
            if node.avail:
                avail_nodes_count += 1
        return avail_nodes_count >= nodes_needed

    def assign_job(self, job):
        node_indices = self.get_avail_node_index(job.node)
        for node_index in node_indices:
            self.nodes[node_index].avail = False
            self.nodes[node_index].time_to_be_avail = self.time + job.act_sec

        job.start_time = self.time

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
        print("\n---jobs start time---")
        for job in self.job_data:
            print("Job %d: %s" % (job.id, formatted_time(job.start_time)))
