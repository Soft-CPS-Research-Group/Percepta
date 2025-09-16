import random, string
from typing import Callable
from threading import Thread
from task_manager.task_group import TaskGroup
from task_manager.task_entity import TaskEntity
from task_manager.repeated_task_name import RepeatedTaskName


# This class allows to manages tasks, such as create, find, delete, and add on a group.
class TaskManager:
    # Each instance has a task group, where tasks are associated.
    __task_group: TaskGroup

    def __init__(self):
        self.__task_group = TaskGroup(''.join(random.choices(string.ascii_letters + string.digits, k=20)))

    # Creates task and associates with the group.
    # It raises:
    #  - RepeatedTaskName if there is a task with this name already.
    #  - ValueError if the callable function is null.
    # Returns the name of the function.
    def create_task(self, func: Callable | object,
                    name: str = ''.join(random.choices(string.ascii_letters + string.digits, k=10))) -> str:

        if func is None:
            raise ValueError()

        if self.__task_group.find_task(name) is not None:
            raise RepeatedTaskName()

        # Creates thread.
        t = Thread(target=func, name=name)

        # Inserts on RB Tree.
        self.__task_group.insert_task(TaskEntity(name, t))

        return name

    # Starts a single task.
    # This is useful for tasks that were added after others tasks were started already.
    # Returns "True" if the task was successfully started (or it was already running), and "False" if the task is not alive or if it doesn't exist.
    def start_task(self, name: str):
        task = self.__task_group.find_task(name)

        if task is None:
            return False

        if task.thread.is_alive():
            return True

        task.thread.run()

        return True

    # Starts all the tasks in the group.
    # Useful for cases where multiple tasks were created at the same time, and need to start at the "same time".
    # Invalid tasks are ignored.
    def start_group(self):

        # Obtains all tasks.
        tasks = self.__task_group.all_tasks()

        # Iterates over tasks and starts each.
        for t in tasks:
            if t is None:
                continue

            t.thread.start()

    def end_task(self):


    # Checks if the threads on this group are executing.
    # Returns the tasks that are not running.
    def check_group_health(self) -> list[TaskEntity]:

        # Stores all the tasks identified as stopped.
        dead_tasks: list[TaskEntity] = []

        all_tasks = self.__task_group.all_tasks()

        for task in all_tasks:
            if task.thread.is_alive() is False:
                dead_tasks.append(task)

        return dead_tasks

    def is_healthy(self) -> bool:
        return len(self.check_group_health()) == 0

    def signal_task(self, signal: int, name: str, task: TaskEntity):
        pass