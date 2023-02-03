"""Module providing custom nodes for PlantUML Flow Builder."""
from typing import Dict, List

from flow import SimpleJoin

IntCtx = Dict[str, int]


class Eval(SimpleJoin[IntCtx, int]):
    """Evaluates Python expression over the given set of variables."""

    def __init__(self, *, inputs: List[str], expression: str) -> None:
        self.expression = expression
        # pylint: disable=W0123
        super().__init__(set(inputs), lambda _, **kw: eval(expression, kw))

    def __repr__(self):
        return f"JN({self.input_names()}, {self.expression})"
