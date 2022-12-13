from abc import abstractmethod
from typing import Callable, TypeVar, Generic, Dict, Tuple, Set
from mypy_extensions import KwArg


# type parameters
CTX = TypeVar('CTX')  # execution context
T = TypeVar('T')  # data type passed between the pipeline nodes


# top level classes

class Node(Generic[CTX, T]):
    """An abstract flow node."""


class Source(Node[CTX, T]):
    """
    And abstraction over a function that takes execution context as the argument
    and returning an instance of T.
    """

    @abstractmethod
    def __call__(self, ctx: CTX) -> T:
        pass

    def to_transformer(self, other: 'Transformer[CTX, T]') -> 'Source[CTX, T]':
        """Connects the output of this source node to the input of a transformer."""
        return SimpleSource(lambda ctx: other(ctx, self(ctx)))

    def __rshift__(self, other: 'Transformer[CTX, T]') -> 'Source[CTX, T]':
        """Alias for `to_transformer` method."""
        return self.to_transformer(other)

    def to_sink(self, other: 'Sink[CTX, T]') -> 'Source[CTX, None]':
        """Connects the output of this source node to the input of a sink."""
        return SimpleSource(lambda ctx: other(ctx, self(ctx)))

    def __gt__(self, other: 'Sink[CTX, T]') -> 'Source[CTX, None]':
        """Alias for `to_sink` method."""
        return self.to_sink(other)

    def to_join(self, other: 'Join[CTX, T]', name: str) -> 'Join[CTX, T]':
        """Connects the output of this source node to an input of a join."""
        new_names = remove_set_item(other.input_names(), name)
        return SimpleJoin(new_names,
                          lambda ctx, **args: other(ctx, **add_dict_entry(args, name, self(ctx))))

    def __ge__(self, other: 'Tuple[Join[CTX, T], str]') -> 'Join[CTX, T]':
        """Alias for `to_join` method."""
        return self.to_join(other[0], other[1])


class SimpleSource(Source[CTX, T]):
    """A simple implementation of Source interface based on a function passed into constructor."""

    def __init__(self, func: Callable[[CTX], T]) -> None:
        self.underlying = func

    def __call__(self, ctx: CTX) -> T:
        return self.underlying(ctx)


class Transformer(Node[CTX, T]):
    """
    And abstraction over a function with 2 arguments: the context and instance of type T,
    which returns an instance of T.
    """

    @abstractmethod
    def __call__(self, ctx: CTX, arg: T) -> T:
        pass

    def to_transformer(self, other: 'Transformer[CTX, T]') -> 'Transformer[CTX, T]':
        """Connects the output of this transformer node to the input of another transformer."""
        return SimpleTransformer(lambda ctx, arg: other(ctx, self(ctx, arg)))

    def __rshift__(self, other: 'Transformer[CTX, T]') -> 'Transformer[CTX, T]':
        """Alias for `to_transformer` method."""
        return self.to_transformer(other)

    def to_sink(self, other: 'Sink[CTX, T]') -> 'Sink[CTX, T]':
        """Connects the output of this transformer node to the input of a sink."""
        return SimpleSink(lambda ctx, arg: other(ctx, self(ctx, arg)))

    def __gt__(self, other: 'Sink[CTX, T]') -> 'Sink[CTX, T]':
        """Alias for `to_sink` method."""
        return self.to_sink(other)

    def to_join(self, other: 'Join[CTX, T]', name: str, new_name: str = None) -> 'Join[CTX, T]':
        """Connects the output of this transformer node to an input of a join."""
        new_name = new_name or name
        new_names = add_set_items(remove_set_item(other.input_names(), name), new_name)
        return SimpleJoin(new_names,
                          lambda ctx, **args: (
                              other(ctx, **add_dict_entry(args, name, self(ctx, args[new_name])))
                          ))

    def __ge__(self, other: 'Tuple[Join[CTX, T], str, str]') -> 'Join[CTX, T]':
        """Alias for `to_join` method."""
        new_name = other[2] if len(other) > 2 else None
        return self.to_join(other[0], other[1], new_name)


class SimpleTransformer(Transformer[CTX, T]):
    """A simple implementation of Transformer interface based on a function passed into constructor."""

    def __init__(self, func: Callable[[CTX, T], T]) -> None:
        self.underlying = func

    def __call__(self, ctx: CTX, arg: T) -> T:
        return self.underlying(ctx, arg)


class Sink(Node[CTX, T]):
    """
    And abstraction over a function with 2 arguments: the context and instance of type T,
    which returns nothing.
    """

    @abstractmethod
    def __call__(self, ctx: CTX, arg: T) -> None:
        pass


class SimpleSink(Sink[CTX, T]):
    """A simple implementation of Sink interface based on a function passed into constructor."""

    def __init__(self, func: Callable[[CTX, T], None]) -> None:
        self.underlying = func

    def __call__(self, ctx: CTX, arg: T) -> None:
        self.underlying(ctx, arg)


class Join(Node[CTX, T]):
    """
    And abstraction over a function with multiple arguments: the context and instances of type T,
    which returns an instance of T.
    """

    def input_names(self) -> Set[str]:
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

    def to_sink(self, other: 'Sink[CTX, T]') -> 'Join[CTX, T]':
        """Connects the output of this join node to the input of a sink."""
        return SimpleJoin(self.input_names(), lambda ctx, **args: other(ctx, self(ctx, **args)))

    def __gt__(self, other: 'Sink[CTX, T]') -> 'Join[CTX, T]':
        """Alias for `to_sink` method."""
        return self.to_sink(other)

    def to_join(self, other: 'Join[CTX, T]', name: str,
                inputs_remap: Dict[str, str] = None) -> 'Join[CTX, T]':
        """Connects the output of this join node to an input of another join."""

        inputs_remap = inputs_remap or {}
        new2old = dict(((inputs_remap.get(name) or name), name) for name in self.input_names())

        all_names = add_set_items(remove_set_item(other.input_names(), name), *set(new2old.keys()))

        def func(ctx: CTX, **args: T) -> T:
            own_args = dict((old_name, args[new_name]) for new_name, old_name in new2old.items())
            args[name] = self(ctx, **own_args)
            return other(ctx, **args)

        return SimpleJoin(all_names, func)

    def __ge__(self, other: 'Tuple[Join[CTX, T], str, Dict[str, str]]') -> 'Join[CTX, T]':
        """Alias for `to_join` method."""
        remap = other[2] if len(other) > 2 else None
        return self.to_join(other[0], other[1], remap)


class SimpleJoin(Join[CTX, T]):
    """A simple implementation of Join interface based on a function passed into constructor."""

    def __init__(self, names: Set[str], func: Callable[[CTX, KwArg(T)], T]) -> None:
        self.names = names
        self.underlying = func

    def input_names(self) -> Set[str]:
        return self.names

    def __call__(self, ctx: CTX, **args: T) -> T:
        return self.underlying(ctx, **args)


def add_set_items(items: Set[str], *to_add: str) -> Set[str]:
    new_items = items.copy()
    for item in to_add:
        new_items.add(item)
    return new_items


def remove_set_item(items: Set[str], item: str) -> Set[str]:
    new_items = items.copy()
    new_items.remove(item)
    return new_items


def add_dict_entry(dct: Dict[str, T], key: str, value: T) -> Dict[str, T]:
    dct2 = dct.copy()
    dct2[key] = value
    return dct2
