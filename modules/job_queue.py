from typing import List, Optional
from collections import deque
from modules.job import Job


class JobQueue:
    def __init__(self, jobs: List[Job]):
        self.queue = deque(jobs)

    def print_queue(self):
        print("*** Job Queue ***")
        if not self.queue:
            print("Empty")
        for job in self.queue:
            print(job)

    def enqueue(self, job: Job):
        self.queue.append(job)

    def dequeue(self) -> Optional[Job]:
        if self.queue:
            return self.queue.popleft()
        return None

    def peek(self, n: int) -> List[Job]:
        return list(self.queue)[:n]

    def is_empty(self) -> bool:
        return len(self.queue) == 0
