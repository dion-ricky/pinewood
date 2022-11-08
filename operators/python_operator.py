from typing import List, Dict

from models.base_operator import BaseOperator
from models.context import Context


class PythonOperator(BaseOperator):
    def __init__(
            self,
            task_id: str,
            python_callable,
            args: List = [],
            kwargs: Dict = {},
            provide_context: bool = True,
            **_kwargs
        ):
        super().__init__(task_id, **_kwargs)
        self.python_callable = python_callable
        self.args = args
        self.kwargs = kwargs
        self.provide_context = provide_context

    def exec(self, context: Context):
        args = self.args
        kwargs = self.kwargs

        if self.provide_context:
            kwargs.update({'context': context})

        self.python_callable(*args, **kwargs)