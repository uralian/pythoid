from abc import abstractmethod
from typing import Callable, TypeVar, Generic, Dict, Tuple, Set, Any

from mypy_extensions import KwArg

# type parameters
CTX = TypeVar('CTX')  # execution context
T = TypeVar('T')  # data type passed between the pipeline nodes


# top level classes

class Node(Generic[CTX, T]):
    """An abstract flow node."""


class Source(Node[CTX, T]):
    """
    An abstraction over a function that takes execution context as the argument
    and returning an instance of T.
    """

    @abstractmethod
    def __call__(self, ctx: CTX) -> T:
        pass

    def to_transformer(self, other: 'Transformer[CTX, T]') -> 'Source[CTX, T]':
        """Connects the output of this source node to the input of a transformer."""
        return SimpleSource(lambda ctx: other(ctx, self(ctx)))

    def __rshift__(self, other: 'Transformer[CTX, T]') -> 'Source[CTX, T]':
        """Alias for `to_transformer` method, allows 'src >> tx' syntax."""
        return self.to_transformer(other)

    def to_join(self, other: 'Join[CTX, T]', name: str) -> 'Join[CTX, T]':
        """Connects the output of this source node to an input of a join."""
        new_names = remove_set_items(other.input_names(), name)
        return SimpleJoin(new_names,
                          lambda ctx, **args: other(ctx, **add_dict_entry(args, name, self(ctx))))

    def __ge__(self, other: 'Tuple[Join[CTX, T], str]') -> 'Join[CTX, T]':
        """Alias for `to_join` method, allows 'src >= (join, name)' syntax."""
        return self.to_join(other[0], other[1])

    def to_sink_t(self, other: 'SinkT[CTX, T]') -> 'SinkS[CTX, T]':
        """Connects the output of this source node to the input of a SinkT."""
        return SimpleSinkS(lambda ctx: other(ctx, self(ctx)))

    def __gt__(self, other: 'SinkT[CTX, T]') -> 'SinkS[CTX, T]':
        """Alias for `to_sink_t` method, allows 'src > sink' syntax."""
        return self.to_sink_t(other)

    def to_sink_j(self, other: 'SinkJ[CTX, T]', name: str) -> 'SinkJ[CTX, T]':
        """Connects the output of this source node to an input of a SinkJ."""
        new_names = remove_set_items(other.input_names(), name)
        return SimpleSinkJ(new_names,
                           lambda ctx, **args: other(ctx, **add_dict_entry(args, name, self(ctx))))

    def __or__(self, other: 'Tuple[SinkJ[CTX, T], str]') -> 'SinkJ[CTX, T]':
        """Alias for `to_sink_j` method, allows 'src | sink' syntax."""
        return self.to_sink_j(other[0], other[1])


class SimpleSource(Source[CTX, T]):
    """A simple implementation of Source interface based on a function passed into constructor."""

    def __init__(self, func: Callable[[CTX], T]) -> None:
        self.underlying = func

    def __call__(self, ctx: CTX) -> T:
        return self.underlying(ctx)


class Transformer(Node[CTX, T]):
    """
    An abstraction over a function with 2 arguments: the context and instance of type T,
    which returns an instance of T.
    """

    @abstractmethod
    def __call__(self, ctx: CTX, arg: T) -> T:
        pass

    def to_transformer(self, other: 'Transformer[CTX, T]') -> 'Transformer[CTX, T]':
        """Connects the output of this transformer node to the input of another transformer."""
        return SimpleTransformer(lambda ctx, arg: other(ctx, self(ctx, arg)))

    def __rshift__(self, other: 'Transformer[CTX, T]') -> 'Transformer[CTX, T]':
        """Alias for `to_transformer` method, allows 'tx1 >> tx2' syntax."""
        return self.to_transformer(other)

    def to_join(self, other: 'Join[CTX, T]', name: str, new_name: str = None) -> 'Join[CTX, T]':
        """Connects the output of this transformer node to an input of a join."""
        new_name = new_name or name
        new_names = add_set_items(remove_set_items(other.input_names(), name), new_name)
        return SimpleJoin(new_names,
                          lambda ctx, **args: (
                              other(ctx, **add_dict_entry(args, name, self(ctx, args[new_name])))
                          ))

    def __ge__(self, other: 'Tuple[Join[CTX, T], str, str]') -> 'Join[CTX, T]':
        """Alias for `to_join` method, allows for 'tx >= (join, name, new_name)' syntax."""
        new_name = other[2] if len(other) > 2 else None
        return self.to_join(other[0], other[1], new_name)

    def to_sink_t(self, other: 'SinkT[CTX, T]') -> 'SinkT[CTX, T]':
        """Connects the output of this transformer node to the input of a SinkT."""
        return SimpleSinkT(lambda ctx, arg: other(ctx, self(ctx, arg)))

    def __gt__(self, other: 'SinkT[CTX, T]') -> 'SinkT[CTX, T]':
        """Alias for `to_sink_t` method, allows 'src > sink' syntax."""
        return self.to_sink_t(other)

    def to_sink_j(self, other: 'SinkJ[CTX, T]', name: str, new_name: str = None) -> 'SinkJ[CTX, T]':
        """Connects the output of this transformer node to an input of a SinkJ."""
        new_name = new_name or name
        new_names = add_set_items(remove_set_items(other.input_names(), name), new_name)
        return SimpleSinkJ(new_names,
                           lambda ctx, **args: (
                               other(ctx, **add_dict_entry(args, name, self(ctx, args[new_name])))
                           ))

    def __or__(self, other: 'Tuple[SinkJ[CTX, T], str, str]') -> 'SinkJ[CTX, T]':
        """Alias for `to_sink_j` method, allows 'src | (sink, name, new_name)' syntax."""
        new_name = other[2] if len(other) > 2 else None
        return self.to_sink_j(other[0], other[1], new_name)


class SimpleTransformer(Transformer[CTX, T]):
    """A simple implementation of Transformer interface based on a function passed into constructor."""

    def __init__(self, func: Callable[[CTX, T], T]) -> None:
        self.underlying = func

    def __call__(self, ctx: CTX, arg: T) -> T:
        return self.underlying(ctx, arg)


class Join(Node[CTX, T]):
    """
    And abstraction over a function with multiple arguments: the context and instances of type T,
    which returns an instance of T.
    """

    def input_names(self) -> Set[str]:
        """Returns the names of this node's inputs."""
        pass

    @abstractmethod
    def __call__(self, ctx: CTX, **args: T) -> T:
        pass

    def to_transformer(self, other: 'Transformer[CTX, T]') -> 'Join[CTX, T]':
        """Connects the output of this join node to the input of a transformer."""
        return SimpleJoin(self.input_names(), lambda ctx, **args: other(ctx, self(ctx, **args)))

    def __rshift__(self, other: 'Transformer[CTX, T]') -> 'Join[CTX, T]':
        """Alias for `to_transformer` method."""
        return self.to_transformer(other)

    def to_join(self, other: 'Join[CTX, T]', name: str,
                inputs_remap: Dict[str, str] = None) -> 'Join[CTX, T]':
        """Connects the output of this join node to an input of another join."""

        inputs_remap = inputs_remap or {}
        new2old = dict(((inputs_remap.get(name) or name), name) for name in self.input_names())

        all_names = add_set_items(remove_set_items(other.input_names(), name), *set(new2old.keys()))

        def func(ctx: CTX, **args: T) -> T:
            own_args = dict((old_name, args[new_name]) for new_name, old_name in new2old.items())
            args[name] = self(ctx, **own_args)
            return other(ctx, **args)

        return SimpleJoin(all_names, func)

    def __ge__(self, other: 'Tuple[Join[CTX, T], str, Dict[str, str]]') -> 'Join[CTX, T]':
        """Alias for `to_join` method, allows 'join1 >= (join2, name, remap)' syntax."""
        remap = other[2] if len(other) > 2 else None
        return self.to_join(other[0], other[1], remap)

    def to_sink_t(self, other: 'SinkT[CTX, T]') -> 'SinkJ[CTX, T]':
        """Connects the output of this join node to the input of a SinkT."""
        return SimpleSinkJ(self.input_names(), lambda ctx, **args: other(ctx, self(ctx, **args)))

    def __gt__(self, other: 'SinkT[CTX, T]') -> 'SinkJ[CTX, T]':
        """Alias for `to_sink` method, allows 'join > sink' syntax."""
        return self.to_sink_t(other)

    def to_sink_j(self, other: 'SinkJ[CTX, T]', name: str,
                  inputs_remap: Dict[str, str] = None) -> 'SinkJ[CTX, T]':
        """Connects the output of this join node to an input of SinkJ."""

        inputs_remap = inputs_remap or {}
        new2old = dict(((inputs_remap.get(name) or name), name) for name in self.input_names())

        all_names = add_set_items(remove_set_items(other.input_names(), name), *set(new2old.keys()))

        def func(ctx: CTX, **args: T) -> Any:
            own_args = dict((old_name, args[new_name]) for new_name, old_name in new2old.items())
            args[name] = self(ctx, **own_args)
            return other(ctx, **args)

        return SimpleSinkJ(all_names, func)

    def __or__(self, other: 'Tuple[SinkJ[CTX, T], str, Dict[str, str]]') -> 'SinkJ[CTX, T]':
        """Alias for `to_sink_j` method, allows 'join | (sink, name, remap)' syntax."""
        remap = other[2] if len(other) > 2 else None
        return self.to_sink_j(other[0], other[1], remap)


class SimpleJoin(Join[CTX, T]):
    """A simple implementation of Join interface based on a function passed into constructor."""

    def __init__(self, names: Set[str], func: Callable[[CTX, KwArg(T)], T]) -> None:
        self.names = names
        self.underlying = func

    def input_names(self) -> Set[str]:
        return self.names

    def __call__(self, ctx: CTX, **args: T) -> T:
        return self.underlying(ctx, **args)


class SinkS(Node[CTX, T]):
    """An abstraction over a function that takes the context argument and produces only side effects."""

    @abstractmethod
    def __call__(self, ctx: CTX) -> Any:
        pass


class SimpleSinkS(SinkS[CTX, T]):
    """A simple implementation of SinkS interface based on a function passed into constructor."""

    def __init__(self, func: Callable[[CTX], Any]) -> None:
        self.underlying = func

    def __call__(self, ctx: CTX) -> Any:
        return self.underlying(ctx)


class SinkT(Node[CTX, T]):
    """
    An abstraction over a function that takes the context and one argument of type T
    and produces only side effects.
    """

    @abstractmethod
    def __call__(self, ctx: CTX, arg: T) -> Any:
        pass


class SimpleSinkT(SinkT[CTX, T]):
    """A simple implementation of SinkT interface based on a function passed into constructor."""

    def __init__(self, func: Callable[[CTX, T], Any]) -> None:
        self.underlying = func

    def __call__(self, ctx: CTX, arg: T) -> Any:
        return self.underlying(ctx, arg)


class SinkJ(Node[CTX, T]):
    """
    An abstraction over a function that takes the context and multiple arguments of type T
    and produces only side effects.
    """

    def input_names(self) -> Set[str]:
        """Returns the names of this node's inputs."""
        pass

    @abstractmethod
    def __call__(self, ctx: CTX, **args: T) -> Any:
        pass


class SimpleSinkJ(SinkJ[CTX, T]):
    """A simple implementation of SinkJ interface based on a function passed into constructor."""

    def __init__(self, names: Set[str], func: Callable[[CTX, KwArg(T)], Any]) -> None:
        self.names = names
        self.underlying = func

    def input_names(self) -> Set[str]:
        return self.names

    def __call__(self, ctx: CTX, **args: T) -> Any:
        return self.underlying(ctx, **args)


def add_set_items(items: Set[str], *to_add: str) -> Set[str]:
    """Creates a new set by adding new items to an existing one (does not change the source set)."""

    new_items = items.copy()
    for item in to_add:
        new_items.add(item)
    return new_items


def remove_set_items(items: Set[str], *to_remove: str) -> Set[str]:
    """Creates a new set by removing elements from an existing one (does not change the source set)."""

    new_items = items.copy()
    for item in to_remove:
        if item in new_items:
            new_items.remove(item)
    return new_items


def add_dict_entry(dct: Dict[str, T], key: str, value: T) -> Dict[str, T]:
    """Creates a new dictionary by adding a new entry to an existing one (does not change the source dict)."""

    dct2 = dct.copy()
    dct2[key] = value
    return dct2
