from typing import List, Union
from abc import ABC, abstractmethod

from models.context import Context


class BaseOperator(ABC):
    def __init__(self, task_id, **kwargs):
        # Prevent circular import
        from models.pipeline import PipelineContext
        
        self.task_id = task_id
        self.__prev = []
        self.__next = []
        self.pipeline = kwargs.get('pipeline', None) or PipelineContext.get_current_pipeline()
        try:
            self.pipeline.register_task(self)
        except:
            raise Exception("Could not get pipeline from context, please manually assign as parameters.")
        self.__state = 'none'

    @property
    def state(self) -> str:
        return self.__state

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, BaseOperator):
            return False
        return self.task_id == __o.task_id

    def __ne__(self, __o: object) -> bool:
        if not isinstance(__o, BaseOperator):
            return True
        return self.task_id != __o.task_id

    def __lshift__(self, value):
        self.set_upstream(value)
        return value

    def __rlshift__(self, value):
        self.set_downstream(value)
        return self

    def __rshift__(self, value):
        self.set_downstream(value)
        return value

    def __rrshift__(self, value):
        self.set_upstream(value)
        return self

    def set_upstream(self, task: Union[List['BaseOperator'], 'BaseOperator']):
        if task in self.__prev:
            return

        if isinstance(task, BaseOperator):
            self.__prev.append(task)
            task.set_downstream(self)
        elif isinstance(task, list):
            for t in task:
                self.set_upstream(t)
        else:
            raise Exception()

    @property
    def upstream(self) -> List['BaseOperator']:
        """Direct upstream of this task"""
        return self.__prev

    def set_downstream(self, task: Union[List['BaseOperator'], 'BaseOperator']):
        if task in self.__next:
            return
        
        if isinstance(task, BaseOperator):
            self.__next.append(task)
            task.set_upstream(self)
        elif isinstance(task, list):
            for t in task:
                self.set_downstream(t)
        else:
            raise Exception()

    @property
    def downstream(self) -> List['BaseOperator']:
        """Direct downstream of this task"""
        return self.__next

    def pre_exec(self):
        self.__state = 'running'

    def post_exec(self, error_raised):
        self.__state = 'success' if not error_raised else 'failed'

    @abstractmethod
    def exec(self, context: Context):
        raise NotImplementedError("Override exec in subclass")