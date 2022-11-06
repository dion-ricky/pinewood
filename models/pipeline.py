from copy import copy
from collections import deque
from typing import Deque, Union, List

from croniter import croniter

from models.base_operator import BaseOperator
from models.context import Context


class Pipeline:
    def __init__(self, pipeline_id, schedule) -> None:
        self.pipeline_id = pipeline_id
        self.schedule = schedule
        self.tasks: List[BaseOperator] = []

        try:
            self.context = Context()
        except TypeError:
            raise Exception("Pipeline should only be called from main!")

    def __enter__(self):
        PipelineContext.push_context_managed_pipeline(self)
        return self

    def __exit__(self, etype, evalue, traceback):
        PipelineContext.pop_context_managed_pipeline()
        self.exec()

    def register_task(self, task: BaseOperator):
        if isinstance(task, BaseOperator):
            self.tasks.append(task)

    def head(self) -> List[BaseOperator]:
        # Head is list of task which are at the top of the dependency graph
        _head = []
        for task in self.tasks:
            if len(task.upstream) == 0:
                _head.append(task)
        return _head

    def is_valid_scheduled_run(self):
        execution_date = self.context.execution_date
        # Snap to xx:00:00:00 due to delay in trigger.
        # Override this if the schedule is minute-specific.
        execution_date.replace(minute=0, second=0, microsecond=0)
        return croniter.match(self.schedule, execution_date)

    def exec(self):
        context = self.context
        if not self.is_valid_scheduled_run():
            return

        def _exec_task(task: BaseOperator, context):
            error_raised = False
            task.pre_exec()
            try:
                task.exec(context)
            except Exception as e:
                error_raised = True
                raise e
            finally:
                task.post_exec(error_raised)

        def _is_task_valid_to_run(task: BaseOperator):
            status = []
            for t in task.upstream:
                status.append(True if t.state == 'success' else False)
            return all(status)

        def clean(tasks: List[BaseOperator]) -> List[BaseOperator]:
            _tasks = []
            for t in tasks:
                if t not in _tasks:
                    _tasks.append(t)
            return _tasks

        heads = self.head()

        # Always assume entrypoint is always valid to run
        next = heads

        while _n := clean(next):
            next = []
            for t in _n:
                if _is_task_valid_to_run(t):
                    _exec_task(t, context)
                    next.extend(t.downstream)
                else:
                    next.append(t)


class PipelineContext:
    _context_managed_pipelines: Deque[Pipeline] = deque()

    @classmethod
    def push_context_managed_pipeline(cls, pipeline: Pipeline) -> None:
        cls._context_managed_pipelines.appendleft(pipeline)

    @classmethod
    def pop_context_managed_pipeline(cls) -> Pipeline:
        return cls._context_managed_pipelines.popleft()

    @classmethod
    def get_current_pipeline(cls) -> Union[Pipeline, None]:
        try:
            return cls._context_managed_pipelines[0]
        except IndexError:
            return None