from typing import List
import numpy as np
from modules.job_queue import JobQueue
from modules.resource import Resource, NodeState
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
        self.jobs_in_schedule = []

    def print_schedule(self):
        print("*** Schedule ***")
        print(self.resource_map)

    def create_initial_schedule(
        self, job_queue: JobQueue, resource: Resource, jobs: List[Job]
    ):
        # キューに入ったタイムステップを設定
        job_queue.set_queued_timestep(self.timestep, self.watch_job_size)
        # FCFS scheduling
        self._schedule_fcfs(job_queue, resource)
        # Backfill scheduling
        self._backfill(job_queue, resource)
        # Allocate resources
        self._allocate_resources(resource, jobs)

    def proceed_timestep(
        self, job_queue: JobQueue, resource: Resource, jobs: List[Job]
    ):
        """タイムステップを1進め、実行中のジョブの状態を更新する"""
        # 現在のタイムステップを1つ進める
        self.timestep += 1

        # キューに入ったタイムステップを設定
        job_queue.set_queued_timestep(self.timestep, self.watch_job_size)

        resource.proceed_timestep()
        completed_jobs = resource.get_completed_jobs()
        running_jobs = resource.get_running_jobs()

        # 完了したジョブをリソースマップから削除
        for job in completed_jobs:
            self.resource_map[self.resource_map == job.job_index] = -1
            self.jobs_in_schedule.remove(job)

        # 実行中かつリソースマップ上で残り時間のあるジョブについて、残り時間をリソースマップ上で更新
        for job in running_jobs:
            scheduled_remaining_timestep = job.allocated_nodes[
                0
            ].scheduled_remaining_timestep
            if scheduled_remaining_timestep == 0:
                continue
            self.resource_map[
                job.allocated_node_indecies, scheduled_remaining_timestep
            ] = -1
            job.occupied_range[1] -= 1

        # リソースマップ上で待機しているジョブについて、前詰めスケジューリングを行う
        # jobs_in_scheduleをリソースマップ上の早い順に並び替え
        self.jobs_in_schedule.sort(key=lambda x: x.occupied_range[0])
        for job in self.jobs_in_schedule:
            start, end = job.occupied_range
            if start == 0:
                continue
            self.resource_map[job.allocated_node_indecies, start:end] = -1
            while (
                all(self.resource_map[job.allocated_node_indecies, start - 1] == -1)
                and start > 0
            ):
                start -= 1
            end = start + job.timestep_length

            self.resource_map[job.allocated_node_indecies, start:end] = job.job_index
            job.occupied_range = [start, end]

        # スケジュール
        self._schedule_fcfs(job_queue, resource)
        self._backfill(job_queue, resource)

        # アイドル中のノードについて、リソースマップ上の先頭のジョブを割り当てる
        self._allocate_resources(resource, jobs)

    def _schedule_fcfs(self, job_queue, resource):
        """Implement FCFS scheduling."""
        jobs_to_remove = []
        # job_queueの先頭からwatch_job_size分のジョブしか見えていない想定
        for job in list(job_queue.queue)[: self.watch_job_size]:
            # 割り当て不可能なジョブが現れたらエラーを投げる
            if (
                job.node_size > self.node_size
                or job.timestep_length > self.timestep_window
            ):
                raise ValueError(
                    f"Job {job.job_index} is too large to fit in the schedule"
                )
            # Find the earliest timestep where this job can start
            start_timestep = self._find_earliest_start_time(job)
            if start_timestep is None:
                # スケジュールできないジョブが現れた時点で終了
                break
            else:
                self._assign_job(job, start_timestep, resource)
                job.scheduled_timestep = self.timestep + start_timestep
                job.is_backfilled = False
                job.occupied_range = [
                    start_timestep,
                    start_timestep + job.timestep_length,
                ]
                # keep track of jobs to remove from the queue
                jobs_to_remove.append(job)
                self.jobs_in_schedule.append(job)
        # Remove scheduled jobs from the queue
        for job in jobs_to_remove:
            job_queue.queue.remove(job)

    def _backfill(self, job_queue: JobQueue, resource: Resource):
        """Implement backfill scheduling."""
        # FCFSスケジューリングで割り当てられなかったジョブを探す
        # バックフィル対象はjob_queueの先頭からwatch_job_size分
        for job in list(job_queue.queue)[: self.watch_job_size]:
            # バックフィルウィンドウの範囲でバックフィルで利用可能なスペースを探す
            for t in range(self.backfill_timestep_window):
                # このタイムステップでジョブが実行可能かどうかを確認
                if self._can_fit_job_in_timeslot(job, t):
                    # ジョブをスケジュールに割り当てる
                    self._assign_job(job, t, resource)
                    job.scheduled_timestep = self.timestep + t
                    job.is_backfilled = True
                    job.occupied_range = [t, t + job.timestep_length]
                    self.jobs_in_schedule.append(job)
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

    def _assign_job(self, job, start_timestep, resource: Resource):
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
                job.allocated_node_indecies.append(node_index)
                job.allocated_nodes.append(resource.nodes[node_index])

                # 必要なノード数が確保できたかチェック
                if len(job.allocated_node_indecies) >= job.node_size:
                    break
        else:
            # 必要なノード数を満たすまでに利用可能なノードが見つからなかった場合の処理
            # この場合、ジョブはスケジュールされないか、異なるアプローチが必要
            print(f"Error: Unable to allocate sufficient nodes for job {job.job_index}")

    def _allocate_resources(self, resource: Resource, jobs: List[Job]):
        """リソースマップの先頭のジョブをリソースに割り当てる"""
        for node_index, node in enumerate(resource.nodes):
            job_index = self.resource_map[node_index, 0]
            if (
                node.state == NodeState.IDLE and job_index != -1
            ):  # ジョブが割り当てられている場合
                job = jobs[job_index]
                node.allocate_job(job)
