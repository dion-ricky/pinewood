class Comms:
    def __init__(self) -> None:
        self.__store = {}

    def push(self, task_id, key, value):
        if task_id in self.__store:
            self.__store[f"{task_id}"].update({
                f"{key}": value
            })
        else:
            self.__store.update({
                f"{task_id}": {
                    f"{key}": value
                }
            })

    def pull(self, task_id, key):
        return self.__store.get(task_id, {}).get(key, None)