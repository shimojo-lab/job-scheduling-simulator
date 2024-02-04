class Resource:
    def __init__(self, node_size) -> None:
        self.nodes = [Node() for _ in range(node_size)]

    def print_nodes(self):
        print("*** Nodes ***")
        for i, node in enumerate(self.nodes):
            print(f"Node {i}: {node.job_index} - {node.remaining_time}")


class Node:
    def __init__(self):
        self.job_index = None
        self.remaining_time = 0
