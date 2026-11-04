from task_manager.rbtree.rbtree import RedBlackTree
from task_manager.task_entity import TaskEntity


# Group where a set of tasks belong. These tasks are managed by a Red-Black Tree and designated by a name.
class TaskGroup:
    # Name of the group.
    name: str

    # Data structure to store tasks.
    group: RedBlackTree

    def __init__(self, name: str):
        self.name = name
        self.group = RedBlackTree()

    # Allows to insert a task on the group.
    def insert_task(self, task: TaskEntity):
        self.group.insert(task)
        return True

    # Removes task from the group.
    def remove_task(self, name: str = '', task: TaskEntity = None):
        self.group.delete(task)

    # Finds task from group.
    def find_task(self, name: str) -> TaskEntity | None:
        return self.group.find(name)

    # Returns all tasks of the group.
    def all_tasks(self) -> list[TaskEntity]:
        return self.group.inorder_traversal()