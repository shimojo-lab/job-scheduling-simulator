from typing import List
from enum import Enum
import copy
import sys

from utils import formatted_time


class Job:
    def __init__(
        self,
        id,
        node_amount,
        req_runtime,
        act_runtime,
        submitted_time_offset,
        pred_runtime,
    ):
        self.id = id
        self.node_amount = node_amount
        self.req_runtime = req_runtime
        self.act_runtime = act_runtime
        self.pred_runtime = pred_runtime
        self.submitted_time_offset = submitted_time_offset
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
        self.job_id = -1


class JobScheduler:
    def __init__(self, nodes_count, job_data):
        self.job_data: List[Job] = job_data
        self.nodes: List[Node] = [Node() for _ in range(nodes_count)]
        self.time = 0

    def reset(self):
        self.time = 0
        self.nodes = [Node() for _ in range(len(self.nodes))]

    def run(self):
        # 実際のジョブの開始時間を計算
        self.schedule(SchedTimeType.ACT, self.job_data)
        print("actual job start time calculated")
        # 各ジョブについて、スケジュールされた開始時刻と予測した開始時刻を計算
        for job in self.job_data:
            """そのジョブの投入時刻を取得"""
            submitted_time = job.start_time_act - job.submitted_time_offset
            print(
                "job %s submitted time: %s" % (job.id, formatted_time(submitted_time))
            )
            # もし投入時刻が負の値ならスキップ
            if submitted_time < 0:
                job.start_time_sched = -1
                job.start_time_pred = -1
                print("job %s is skipped" % job.id)
                continue
            """その時刻の実際のジョブの状態を取得"""
            remaining_jobs = copy.deepcopy(self.job_data)
            # 終了したジョブを除外
            remaining_jobs = [
                j
                for j in remaining_jobs
                if j.start_time_act + j.act_runtime > submitted_time
            ]
            # 実行中のジョブの残り実行時間を計算
            for rj in remaining_jobs:
                if rj.start_time_act < submitted_time:
                    rj.req_runtime -= submitted_time - rj.start_time_act
                    rj.pred_runtime -= submitted_time - rj.start_time_act
            """スケジューリングをシミュレートしジョブの開始時刻をセット"""
            # スケジュールされたジョブの開始時間を計算
            job.start_time_sched = self.schedule(
                SchedTimeType.SCHED, remaining_jobs, job.id, submitted_time
            )
            print(
                "scheduled job %s start time calculated: %s"
                % (job.id, formatted_time(job.start_time_sched))
            )
            # PREDではジョブの残り実行時間が0以下の場合があるので、そのようなジョブを除外
            remaining_jobs_cp = copy.deepcopy(remaining_jobs)
            remaining_jobs = [j for j in remaining_jobs_cp if j.pred_runtime > 0]
            # 予測されたジョブの開始時間を計算
            job.start_time_pred = self.schedule(
                SchedTimeType.PRED, remaining_jobs, job.id, submitted_time
            )
            print(
                "predicted job %s start time calculated: %s"
                % (job.id, formatted_time(job.start_time_pred))
            )

    # スケジューリングを行い、最後に割り当てたジョブの開始時間を返す
    def schedule(
        self,
        sched_time_type: SchedTimeType,
        job_data: List[Job],
        target_job_id: str = "",
        base_time: int = 0,
    ) -> int:
        self.time = base_time
        if sched_time_type != SchedTimeType.ACT:
            job_data = copy.deepcopy(job_data)  # 元の配列を変更しないためにコピー
        for job in job_data:
            while True:
                # if self.time - base_time >= 50:
                #     print("\033[1;31m job allocation timeout\033[0m")
                #     self.reset()
                #     return -1
                if self.check_nodes(job.node_amount):
                    # 計算の進行状況を10%ごとに出力
                    # if job.id % (len(job_data) // 10) == 0:
                    #     print("%d%%" % (job.id / len(job_data) * 100))
                    self.assign_job(job, sched_time_type)
                    # ジョブの割り当て情報を表示
                    # print(
                    #     "%s job%s (%d node, %d sec) assigned"
                    #     % (
                    #         formatted_time(self.time),
                    #         job.id,
                    #         job.node_amount,
                    #         job.req_runtime,
                    #     )
                    # )
                    # SCHEDまたはPREDの場合はtarget_jobに到達した時点で終了
                    if sched_time_type != SchedTimeType.ACT and job.id == target_job_id:
                        ret = self.time
                        self.reset()
                        return ret
                    break
                else:
                    self.time += self.get_min_time_to_be_avail()
                    self.update_nodes()
        self.reset()
        return self.time

    # ジョブがノードに割り当て可能かどうかをチェックする
    def check_nodes(self, nodes_needed):
        # 各ノードに割り当てられているジョブを表示
        # print(
        #     "check_nodes: %s : %d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d"
        #     % (
        #         formatted_time(self.time),
        #         self.nodes[0].job_id,
        #         self.nodes[1].job_id,
        #         self.nodes[2].job_id,
        #         self.nodes[3].job_id,
        #         self.nodes[4].job_id,
        #         self.nodes[5].job_id,
        #         self.nodes[6].job_id,
        #         self.nodes[7].job_id,
        #         self.nodes[8].job_id,
        #         self.nodes[9].job_id,
        #     )
        # )
        avail_nodes_count = 0
        for node in self.nodes:
            if node.avail:
                avail_nodes_count += 1
        return avail_nodes_count >= nodes_needed

    def assign_job(self, job: Job, sched_time: SchedTimeType):
        node_indices = self.get_avail_node_index(job.node_amount)
        # ノードの利用状況を更新
        for node_index in node_indices:
            self.nodes[node_index].avail = False
            self.nodes[node_index].job_id = job.id
            if sched_time == SchedTimeType.ACT:
                lapse_runtime = job.act_runtime
            elif sched_time == SchedTimeType.SCHED:
                lapse_runtime = job.req_runtime
            elif sched_time == SchedTimeType.PRED:
                lapse_runtime = job.pred_runtime
            self.nodes[node_index].time_to_be_avail = self.time + lapse_runtime
        # ジョブの開始時間を記録
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
                node.job_id = -1

    # 現在使用中のノードの中で最も早く空きができるノードの開放時間を返す
    def get_min_time_to_be_avail(self):
        min_time = sys.maxsize
        for node in self.nodes:
            if not node.avail and node.time_to_be_avail < min_time:
                min_time = node.time_to_be_avail
        return min_time - self.time

    def show_jobs_start_time(self):
        print("\n---jobs start time---")
        print("Job ID\tact\t\tsched\t\tpred")
        for job in self.job_data:
            print(
                "%d\t%s\t%s\t%s"
                % (
                    job.id,
                    formatted_time(job.start_time_act),
                    formatted_time(job.start_time_sched),
                    formatted_time(job.start_time_pred),
                )
            )

        # Compute root mean squared error of sched, pred for act
        rmse_sched = 0
        rmse_pred = 0
        count = 0
        for job in self.job_data:
            if job.start_time_sched == -1 or job.start_time_pred == -1:
                continue
            count += 1
            rmse_sched += (job.start_time_sched - job.start_time_act) ** 2
            rmse_pred += (job.start_time_pred - job.start_time_act) ** 2
        rmse_sched /= count
        rmse_pred /= count
        rmse_sched = rmse_sched**0.5
        rmse_pred = rmse_pred**0.5
        print("%d jobs used for RMSE" % count)
        print("\n---RMSE---")
        print("\t\t\t%f\t%f" % (rmse_sched, rmse_pred))
