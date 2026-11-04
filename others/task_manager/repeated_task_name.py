class RepeatedTaskName(Exception):

    def __init__(self):
        super().__init__("There is a task with this name already")