from threading import Thread


# Task object containing a name and a thread.
class TaskEntity:
    name: str
    thread: Thread

    def __init__(self, name: str, thread: Thread):
        self.name = name
        self.thread = thread

    def __lt__(self, other):
        return self.name < other.name

    def __eq__(self, other):
        return self.name == other.name