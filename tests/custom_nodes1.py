"""Module providing custom nodes for PlantUML Flow Builder."""
from typing import Dict

from flow import SimpleSource, SimpleTransformer

IntCtx = Dict[str, int]


class GetContext(SimpleSource[IntCtx, int]):
    """Retrieves value from context."""

    def __init__(self, *, key: str) -> None:
        self.key = key
        super().__init__(lambda ctx: ctx[key])

    def __repr__(self):
        return f"SRC({self.key})"


class Multiplier(SimpleTransformer[IntCtx, int]):
    """Multiplies input by the specified value."""

    def __init__(self, *, value: int) -> None:
        self.value = value
        super().__init__(lambda _, x: x * value)

    def __repr__(self):
        return f"TX(x * {self.value})"


class Adder(SimpleTransformer[IntCtx, int]):
    """Adds the specified value to input."""

    def __init__(self, *, value: int) -> None:
        self.value = value
        super().__init__(lambda _, x: x + value)

    def __repr__(self):
        return f"TX(x + {self.value})"


class Remainder(SimpleTransformer[IntCtx, int]):
    """Computes a remainder from division of input by the specified value."""

    def __init__(self, *, value: int) -> None:
        self.value = value
        super().__init__(lambda _, x: x % value)

    def __repr__(self):
        return f"TX(x % {self.value})"
