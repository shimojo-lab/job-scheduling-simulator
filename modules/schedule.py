from typing import List
import numpy as np
from modules.job_queue import JobQueue
from modules.resource import Resource
from modules.job import Job


class Schedule:
    def __init__(
        self,
        node_size: int,
        timestep_window: int,
        backfill_timestep_window: int,
        timestep_seconds: int,
        watch_job_size: int,
    ):
        self.timestep = 0
        self.node_size = node_size
        self.timestep_window = timestep_window
        self.backfill_timestep_window = backfill_timestep_window
        self.timestep_seconds = timestep_seconds
        self.resource_map = np.full((node_size, timestep_window), -1)
        self.watch_job_size = watch_job_size

    def print_schedule(self):
        print("*** Schedule ***")
        print(self.resource_map)

    def create_initial_schedule(
        self, job_queue: JobQueue, resource: Resource, jobs: List[Job]
    ):
        # FCFS scheduling
        self._schedule_fcfs(job_queue)
        # Backfill scheduling
        self._backfill(job_queue)
        # Allocate resources
        self._allocate_resources(resource, jobs)

    def _schedule_fcfs(self, job_queue):
        """Implement FCFS scheduling."""
        jobs_to_remove = []
        for job in job_queue.queue:
            # Find the earliest timestep where this job can start
            start_timestep = self._find_earliest_start_time(job)
            if start_timestep is None:
                # スケジュールできないジョブが現れた時点で終了
                break
            else:
                self._assign_job(job, start_timestep)
                job.scheduled_timestep = start_timestep
                job.is_backfilled = False
                # keep track of jobs to remove from the queue
                jobs_to_remove.append(job)
        # Remove scheduled jobs from the queue
        for job in jobs_to_remove:
            job_queue.queue.remove(job)

    def _backfill(self, job_queue: JobQueue):
        """Implement backfill scheduling."""
        # FCFSスケジューリングで割り当てられなかったジョブを探す
        for job in list(job_queue.queue)[: self.watch_job_size]:
            # バックフィルで利用可能なスペースを探す
            for t in range(self.backfill_timestep_window):
                # このタイムステップでジョブが実行可能かどうかを確認
                if self._can_fit_job_in_timeslot(job, t):
                    # ジョブをスケジュールに割り当てる
                    self._assign_job(job, t)
                    job.scheduled_timestep = t
                    job.is_backfilled = True
                    # スケジュールされたジョブをキューから削除
                    job_queue.queue.remove(job)
                    break  # このジョブの処理を終了

    def _can_fit_job_in_timeslot(self, job: Job, start_timestep: int):
        """指定されたタイムステップでジョブが実行可能かどうかを確認"""
        job_pred_timesteps = np.ceil(job.pred_time / self.timestep_seconds).astype(int)
        available_nodes = np.where(self.resource_map[:, start_timestep] == -1)[0]
        if len(available_nodes) < job.node_size:
            return False
        # ジョブの実行時間がバックフィルウィンドウを超える場合
        if job_pred_timesteps + start_timestep > self.backfill_timestep_window:
            return False
        if np.all(
            self.resource_map[
                available_nodes, start_timestep : start_timestep + job_pred_timesteps
            ]
            == -1
        ):
            return True
        return False  # 実行できるスペースがない

    def _find_earliest_start_time(self, job):
        """Find the earliest timestep where the job can start, converting pred_time from seconds to timesteps."""
        # ジョブのpred_timeをタイムステップに変換（秒をタイムステップに変換）
        job_pred_timesteps = np.ceil(job.pred_time / self.timestep_seconds).astype(int)

        for t in range(self.timestep_window - job_pred_timesteps + 1):
            # 各タイムステップで利用可能なノード数をカウント
            available_nodes = np.where(self.resource_map[:, t] == -1)[0]

            if len(available_nodes) < job.node_size:
                # 必要なノード数が利用可能でない場合はスキップ
                continue

            # 指定された期間にわたって、必要なノード数が連続して利用可能かチェック
            consecutive_available = True
            for dt in range(1, job_pred_timesteps):
                if not all(self.resource_map[available_nodes, t + dt] == -1):
                    consecutive_available = False
                    break

            if consecutive_available:
                # すべての条件を満たす場合は、このタイムステップを返す
                return t

        return None  # 適切な開始時間が見つからない場合

    def _assign_job(self, job, start_timestep):
        """ジョブをスケジュールに割り当てる"""
        # ジョブのpred_timeをタイムステップに変換（秒をタイムステップに変換）
        job_pred_timesteps = np.ceil(job.pred_time / self.timestep_seconds).astype(int)

        # 必要なノード数が利用可能であり、かつ連続して利用可能なノードを探す
        for node_index in range(self.node_size):
            if np.all(
                self.resource_map[
                    node_index, start_timestep : start_timestep + job_pred_timesteps
                ]
                == -1
            ):
                # このノードをジョブの開始タイムステップからpred_timeの期間にわたって予約する
                self.resource_map[
                    node_index, start_timestep : start_timestep + job_pred_timesteps
                ] = job.job_index
                job.allocated_nodes.append(node_index)

                # 必要なノード数が確保できたかチェック
                if len(job.allocated_nodes) >= job.node_size:
                    break
        else:
            # 必要なノード数を満たすまでに利用可能なノードが見つからなかった場合の処理
            # この場合、ジョブはスケジュールされないか、異なるアプローチが必要
            print(f"Error: Unable to allocate sufficient nodes for job {job.job_index}")

    def _allocate_resources(self, resource: Resource, jobs: List[Job]):
        """リソースマップの先頭のジョブをリソースに割り当てる"""
        for node_index, node in enumerate(resource.nodes):
            job_index = self.resource_map[node_index, 0]
            if job_index != -1:  # ジョブが割り当てられている場合
                job = jobs[job_index]
                node.job_index = job_index
                node.remaining_time = job.pred_time
