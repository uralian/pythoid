"""Module providing building common blocks for Pythoid dataflows."""
import logging
from abc import abstractmethod
from typing import Any, Callable, Dict, Generic, Optional, Set, Tuple, TypeVar

from mypy_extensions import KwArg

# data types
CTX = TypeVar("CTX")  # execution context
T = TypeVar("T")  # data type passed between the pipeline nodes

# node types
NoInputT = TypeVar("NoInputT", bound="NoInput")
SingleInputT = TypeVar("SingleInputT", bound="SingleInput")
MultiInputT = TypeVar("MultiInputT", bound="MultiInput")

# function aliases
NoInputFunc = Callable[[CTX], T | Any]
SingleInputFunc = Callable[[CTX, T], T | Any]
MultiInputFunc = Callable[[CTX, KwArg(T)], T | Any]


class Node(Generic[CTX, T]):
    """An abstract dataflow node."""

    def __init__(self):
        self.log: logging.Logger  # had to add this to keep mypy happy
        object.__setattr__(self, "log", logging.getLogger(self.__class__.__name__))
        self.log.info("self=%s", self)

    def __repr__(self):
        return f"{self.__class__.__name__}(#{id(self)})"


class NoInput(Node[CTX, T]):
    """A node that has does not have an input other than the execution context."""

    def __call__(self, ctx: CTX):
        self.log.debug("self=%s, ctx=%s", self, ctx)
        result = self._do_call__(ctx)
        self.log.debug("self=%s, result=%s", self, result)
        return result

    @abstractmethod
    def _do_call__(self, ctx: CTX):
        raise NotImplementedError("Must be implemented by subclasses")


class SingleInput(Node[CTX, T]):
    """A node that has a single input."""

    def __call__(self, ctx: CTX, arg: T):
        self.log.debug("self=%s, ctx=%s, arg=%s", self, ctx, arg)
        result = self._do_call__(ctx, arg)
        self.log.debug("self=%s, result=%s", self, result)
        return result

    @abstractmethod
    def _do_call__(self, ctx: CTX, arg: T):
        raise NotImplementedError("Must be implemented by subclasses")


class MultiInput(Node[CTX, T]):
    """A node that has multiple named inputs."""

    def __init__(self, names: Set[str]) -> None:
        object.__setattr__(self, "names", names)
        super().__init__()

    def __call__(self, ctx: CTX, **args: T):
        self.log.debug("self=%s, ctx=%s, args=%s", self, ctx, args)
        result = self._do_call__(ctx, **args)
        self.log.debug("self=%s, result=%s", self, result)
        return result

    @abstractmethod
    def _do_call__(self, ctx: CTX, **args: T):
        raise NotImplementedError("Must be implemented by subclasses")

    def input_names(self) -> Set[str]:
        """Returns input names for this node."""
        return object.__getattribute__(self, "names")

    def input_count(self) -> int:
        """Returns the number of inputs for this node."""
        return len(self.input_names())

    def assert_name_exists(self, name):
        """Raises AssertionError if the specified name is not among the input names."""
        assert (
            name in self.input_names()
        ), f"Input '{name}' not found in node inputs: {self.input_names()}"

    def __repr__(self):
        return f"{self.__class__.__name__}({self.input_names()})"


class Source(NoInput[CTX, T]):
    """
    An abstraction over a function that takes execution context as the argument
    and returning an instance of T.
    """

    @abstractmethod
    def _do_call__(self, ctx: CTX) -> T:
        raise NotImplementedError("Must be implemented by subclasses")

    def to_transformer(self, other: "Transformer[CTX, T]") -> "Source[CTX, T]":
        """Connects the output of this source node to the input of a Transformer."""
        self.log.info("self=%s, other=%s", self, other)
        return self._to_single_input(SimpleSource, other)

    def to_join(self, other: "Join[CTX, T]", name: str) -> "Join[CTX, T]":
        """Connects the output of this source node to an input of a Join."""
        self.log.info("self=%s, other=%s, name=%s", self, other, name)
        return self._to_multi_input(SimpleJoin, other, name)

    def to_stub(self, other: "Stub[CTX, T]") -> "Task[CTX, T]":
        """Connects the output of this source node to the input of a Stub."""
        self.log.info("self=%s, other=%s", self, other)
        return self._to_single_input(SimpleTask, other)

    def to_module(self, other: "Module[CTX, T]", name: str) -> "Module[CTX, T]":
        """Connects the output of this source node to an input of a Module."""
        self.log.info("self=%s, other=%s, name=%s", self, other, name)
        return self._to_multi_input(SimpleModule, other, name)

    def _to_single_input(
        self, cls: Callable[[NoInputFunc], NoInputT], other: SingleInput
    ) -> NoInputT:
        return cls(lambda ctx: other(ctx, self(ctx)))

    def _to_multi_input(
        self,
        cls: Callable[[set[str], MultiInputFunc], MultiInputT],
        other: MultiInput,
        name: str,
    ) -> MultiInputT:
        other.assert_name_exists(name)
        new_names = remove_set_items(other.input_names(), name)
        return cls(
            new_names,
            lambda ctx, **args: other(ctx, **add_dict_entry(args, name, self(ctx))),
        )

    def __rshift__(self, other: "Transformer[CTX, T]") -> "Source[CTX, T]":
        """Alias for `to_transformer` method, allows 'src >> tx' syntax."""
        return self.to_transformer(other)

    def __ge__(self, other: "Tuple[Join[CTX, T], str]") -> "Join[CTX, T]":
        """Alias for `to_join` method, allows 'src >= (join, name)' syntax."""
        return self.to_join(other[0], other[1])

    def __gt__(self, other: "Stub[CTX, T]") -> "Task[CTX, T]":
        """Alias for `to_stub` method, allows 'src > sink' syntax."""
        return self.to_stub(other)

    def __or__(self, other: "Tuple[Module[CTX, T], str]") -> "Module[CTX, T]":
        """Alias for `to_module` method, allows 'src | sink' syntax."""
        return self.to_module(other[0], other[1])


class SimpleSource(Source[CTX, T]):
    """A simple implementation of Source interface based on a function passed into constructor."""

    def __init__(self, func: Callable[[CTX], T]) -> None:
        self.underlying = func
        super().__init__()

    def _do_call__(self, ctx: CTX) -> T:
        return self.underlying(ctx)


class Transformer(SingleInput[CTX, T]):
    """
    An abstraction over a function with 2 arguments: the context and instance of type T,
    which returns an instance of T.
    """

    @abstractmethod
    def _do_call__(self, ctx: CTX, arg: T) -> T:
        raise NotImplementedError("Must be implemented by subclasses")

    def to_transformer(self, other: "Transformer[CTX, T]") -> "Transformer[CTX, T]":
        """Connects the output of this transformer node to the input of another Transformer."""
        self.log.info("self=%s, other=%s", self, other)
        return self._to_single_input(SimpleTransformer, other)

    def to_join(
        self, other: "Join[CTX, T]", name: str, new_name: Optional[str] = None
    ) -> "Join[CTX, T]":
        """Connects the output of this transformer node to an input of a Join."""
        self.log.info(
            "self=%s, other=%s, name=%s, new_name=%s", self, other, name, new_name
        )
        return self._to_multi_input(SimpleJoin, other, name, new_name)

    def to_stub(self, other: "Stub[CTX, T]") -> "Stub[CTX, T]":
        """Connects the output of this transformer node to the input of a Stub."""
        self.log.info("self=%s, other=%s", self, other)
        return self._to_single_input(SimpleStub, other)

    def to_module(
        self, other: "Module[CTX, T]", name: str, new_name: Optional[str] = None
    ) -> "Module[CTX, T]":
        """Connects the output of this transformer node to an input of a Module."""
        self.log.info(
            "self=%s, other=%s, name=%s, new_name=%s", self, other, name, new_name
        )
        return self._to_multi_input(SimpleModule, other, name, new_name)

    def _to_single_input(
        self, cls: Callable[[SingleInputFunc], SingleInputT], other: SingleInput
    ) -> SingleInputT:
        return cls(lambda ctx, arg: other(ctx, self(ctx, arg)))

    def _to_multi_input(
        self,
        cls: Callable[[set[str], MultiInputFunc], MultiInputT],
        other: MultiInput,
        name: str,
        new_name: Optional[str] = None,
    ) -> MultiInputT:
        other.assert_name_exists(name)
        arg_name: str = new_name or name
        new_names = add_set_items(remove_set_items(other.input_names(), name), arg_name)
        return cls(
            new_names,
            lambda ctx, **args: other(
                ctx, **add_dict_entry(args, name, self(ctx, args[arg_name]))
            ),
        )

    def __rshift__(self, other: "Transformer[CTX, T]") -> "Transformer[CTX, T]":
        """Alias for `to_transformer` method, allows 'tx1 >> tx2' syntax."""
        return self.to_transformer(other)

    def __ge__(self, other: "Tuple[Join[CTX, T], str, str]") -> "Join[CTX, T]":
        """Alias for `to_join` method, allows for 'tx >= (join, name, new_name)' syntax."""
        new_name = other[2] if len(other) > 2 else None
        return self.to_join(other[0], other[1], new_name)

    def __gt__(self, other: "Stub[CTX, T]") -> "Stub[CTX, T]":
        """Alias for `to_stub` method, allows 'src > sink' syntax."""
        return self.to_stub(other)

    def __or__(self, other: "Tuple[Module[CTX, T], str, str]") -> "Module[CTX, T]":
        """Alias for `to_module` method, allows 'src | (sink, name, new_name)' syntax."""
        new_name = other[2] if len(other) > 2 else None
        return self.to_module(other[0], other[1], new_name)


class SimpleTransformer(Transformer[CTX, T]):
    """
    A simple implementation of Transformer interface based on a function
    passed into constructor.
    """

    def __init__(self, func: Callable[[CTX, T], T]) -> None:
        self.underlying = func
        super().__init__()

    def _do_call__(self, ctx: CTX, arg: T) -> T:
        return self.underlying(ctx, arg)


class Join(MultiInput[CTX, T]):
    """
    And abstraction over a function with multiple arguments: the context and instances of type T,
    which returns an instance of T.
    """

    @abstractmethod
    def _do_call__(self, ctx: CTX, **args: T) -> T:
        raise NotImplementedError("Must be implemented by subclasses")

    def to_transformer(self, other: "Transformer[CTX, T]") -> "Join[CTX, T]":
        """Connects the output of this join node to the input of a Transformer."""
        self.log.info("self=%s, other=%s", self, other)
        return self._to_single_input(SimpleJoin, other)

    def to_join(
        self,
        other: "Join[CTX, T]",
        name: str,
        inputs_remap: Optional[Dict[str, str]] = None,
    ) -> "Join[CTX, T]":
        """Connects the output of this join node to an input of another Join."""
        self.log.info(
            "self=%s, other=%s, name=%s, remap=%s", self, other, name, inputs_remap
        )
        return self._to_multi_input(SimpleJoin, other, name, inputs_remap)

    def to_stub(self, other: "Stub[CTX, T]") -> "Module[CTX, T]":
        """Connects the output of this join node to the input of a Stub."""
        self.log.info("self=%s, other=%s", self, other)
        return self._to_single_input(SimpleModule, other)

    def to_module(
        self,
        other: "Module[CTX, T]",
        name: str,
        inputs_remap: Optional[Dict[str, str]] = None,
    ) -> "Module[CTX, T]":
        """Connects the output of this join node to an input of Module."""
        self.log.info(
            "self=%s, other=%s, name=%s, remap=%s", self, other, name, inputs_remap
        )
        return self._to_multi_input(SimpleModule, other, name, inputs_remap)

    def _to_single_input(
        self, cls: Callable[[set[str], MultiInputFunc], MultiInputT], other: SingleInput
    ) -> MultiInputT:
        return cls(
            self.input_names(), lambda ctx, **args: other(ctx, self(ctx, **args))
        )

    def _to_multi_input(
        self,
        cls: Callable[[set[str], MultiInputFunc], MultiInputT],
        other: MultiInput,
        name: str,
        inputs_remap: Optional[Dict[str, str]] = None,
    ) -> MultiInputT:
        other.assert_name_exists(name)
        if inputs_remap:
            for key in inputs_remap.keys():
                self.assert_name_exists(key)

        remap: dict[str, str] = inputs_remap or {}
        new2old = dict(((remap.get(name) or name), name) for name in self.input_names())

        all_names = add_set_items(
            remove_set_items(other.input_names(), name), *set(new2old.keys())
        )

        def func(ctx, **args):
            own_args = dict(
                (old_name, args[new_name]) for new_name, old_name in new2old.items()
            )
            args[name] = self(ctx, **own_args)
            return other(ctx, **args)

        return cls(all_names, func)  # type: ignore

    def __rshift__(self, other: "Transformer[CTX, T]") -> "Join[CTX, T]":
        """Alias for `to_transformer` method."""
        return self.to_transformer(other)

    def __ge__(
        self, other: "Tuple[Join[CTX, T], str, Dict[str, str]]"
    ) -> "Join[CTX, T]":
        """Alias for `to_join` method, allows 'join1 >= (join2, name, remap)' syntax."""
        remap = other[2] if len(other) > 2 else None
        return self.to_join(other[0], other[1], remap)

    def __gt__(self, other: "Stub[CTX, T]") -> "Module[CTX, T]":
        """Alias for `to_stub` method, allows 'join > sink' syntax."""
        return self.to_stub(other)

    def __or__(
        self, other: "Tuple[Module[CTX, T], str, Dict[str, str]]"
    ) -> "Module[CTX, T]":
        """Alias for `to_module` method, allows 'join | (sink, name, remap)' syntax."""
        remap = other[2] if len(other) > 2 else None
        return self.to_module(other[0], other[1], remap)


class SimpleJoin(Join[CTX, T]):
    """A simple implementation of Join interface based on a function passed into constructor."""

    def __init__(self, names: Set[str], func: Callable[[CTX, KwArg(T)], T]) -> None:
        self.underlying = func
        super().__init__(names)

    def _do_call__(self, ctx: CTX, **args: T) -> T:
        return self.underlying(ctx, **args)


class Task(NoInput[CTX, T]):
    """
    An abstraction over a function that takes the context argument
    and produces only side effects.
    """

    @abstractmethod
    def _do_call__(self, ctx: CTX) -> Any:
        raise NotImplementedError("Must be implemented by subclasses")


class SimpleTask(Task[CTX, T]):
    """A simple implementation of Task interface based on a function passed into constructor."""

    def __init__(self, func: Callable[[CTX], Any]) -> None:
        self.underlying = func
        super().__init__()

    def _do_call__(self, ctx: CTX) -> Any:
        return self.underlying(ctx)


class Stub(SingleInput[CTX, T]):
    """
    An abstraction over a function that takes the context and one argument of type T
    and produces only side effects.
    """

    @abstractmethod
    def _do_call__(self, ctx: CTX, arg: T) -> Any:
        raise NotImplementedError("Must be implemented by subclasses")


class SimpleStub(Stub[CTX, T]):
    """A simple implementation of Stub interface based on a function passed into constructor."""

    def __init__(self, func: Callable[[CTX, T], Any]) -> None:
        self.underlying = func
        super().__init__()

    def _do_call__(self, ctx: CTX, arg: T) -> Any:
        return self.underlying(ctx, arg)


class Module(MultiInput[CTX, T]):
    """
    An abstraction over a function that takes the context and multiple arguments of type T
    and produces only side effects.
    """

    @abstractmethod
    def _do_call__(self, ctx: CTX, **args: T) -> Any:
        raise NotImplementedError("Must be implemented by subclasses")


class SimpleModule(Module[CTX, T]):
    """A simple implementation of Module interface based on a function passed into constructor."""

    def __init__(self, names: Set[str], func: Callable[[CTX, KwArg(T)], Any]) -> None:
        self.underlying = func
        super().__init__(names)

    def _do_call__(self, ctx: CTX, **args: T) -> Any:
        return self.underlying(ctx, **args)


def add_set_items(items: Set[str], *to_add: str) -> Set[str]:
    """Creates a new set by adding new items to an existing one (does not change the source set)."""

    new_items = items.copy()
    for item in to_add:
        new_items.add(item)
    return new_items


def remove_set_items(items: Set[str], *to_remove: str) -> Set[str]:
    """
    Creates a new set by removing elements from an existing one
    (does not change the source set).
    """

    new_items = items.copy()
    for item in to_remove:
        if item in new_items:
            new_items.remove(item)
    return new_items


def add_dict_entry(dct: Dict[str, T], key: str, value: T) -> Dict[str, T]:
    """
    Creates a new dictionary by adding a new entry to an existing one
    (does not change the source dict).
    """

    dct2 = dct.copy()
    dct2[key] = value
    return dct2
