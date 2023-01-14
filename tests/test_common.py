"""Module providing unit tests for common Pythoid blocks."""
import logging
import unittest
from typing import Any, Callable, Dict

from mypy_extensions import KwArg

from flow import (
    Join,
    Module,
    SimpleJoin,
    SimpleModule,
    SimpleSource,
    SimpleStub,
    SimpleTask,
    SimpleTransformer,
    Source,
    Stub,
    Task,
    Transformer,
)
from flow.common import add_dict_entry, add_set_items, remove_set_items
from tests import LOG_DATE_FORMAT, LOG_MSG_FORMAT

IntCtx = Dict[str, int]
StrCtx = Dict[str, str]


class CommonFunctionsTestCase(unittest.TestCase):
    """A test suite for functions used by common Pythoid classes."""

    def test_add_set_items(self):
        """Tests `add_set_items()` function."""
        src = {"a", "b", "c"}
        tgt = add_set_items(src, "x", "b", "z")
        self.assertSetEqual(src, {"a", "b", "c"})
        self.assertSetEqual(tgt, {"a", "b", "c", "x", "z"})

    def test_remove_set_items(self):
        """Tests `remove_set_items()` function."""
        src = {"a", "b", "c", "d", "e"}
        tgt = remove_set_items(src, "d", "a", "x", "y")
        self.assertSetEqual(src, {"a", "b", "c", "d", "e"})
        self.assertSetEqual(tgt, {"b", "c", "e"})

    def test_add_dict_entry(self):
        """Tests `add_dict_entry()` function."""
        src = {"a": 1, "b": 2, "c": 3}
        tgt1 = add_dict_entry(src, "x", 5)
        tgt2 = add_dict_entry(tgt1, "a", 9)
        self.assertDictEqual(src, {"a": 1, "b": 2, "c": 3})
        self.assertDictEqual(tgt1, {"a": 1, "b": 2, "c": 3, "x": 5})
        self.assertDictEqual(tgt2, {"a": 9, "b": 2, "c": 3, "x": 5})


class SourceTestCase(unittest.TestCase):
    """A test suite for Pythoid Source class."""

    def setUp(self) -> None:
        logging.basicConfig(
            format=LOG_MSG_FORMAT, datefmt=LOG_DATE_FORMAT, level=logging.WARNING
        )

    def test_abstract_source(self):
        """Tests creating an abstract Source instance."""
        src = Source()  # type: ignore
        self.assertRaises(NotImplementedError, src.__call__, None)

    def test_int_source(self):
        """Tests a Source producing integers."""
        src = SimpleSource[IntCtx, int](lambda ctx: ctx["a"])
        self.assertEqual(src({"a": 5, "b": 3}), 5)

    def test_str_source(self):
        """Tests a Source producing strings."""
        func: Callable[[Any], str] = lambda _: "a"
        src = SimpleSource(func)
        self.assertEqual(src(None), "a")


class TransformerTestCase(unittest.TestCase):
    """A test suite for Pythoid Transformer class."""

    def setUp(self) -> None:
        logging.basicConfig(
            format=LOG_MSG_FORMAT, datefmt=LOG_DATE_FORMAT, level=logging.WARNING
        )

    def test_abstract_transformer(self):
        """Tests creating an abstract Transformer instance."""
        tx = Transformer()  # type: ignore
        self.assertRaises(NotImplementedError, tx.__call__, None, 0)

    def test_int_transformer(self):
        """Tests an integer Transformer."""
        func: Callable[[IntCtx, int], int] = lambda ctx, x: x * ctx["b"]
        tx2 = SimpleTransformer(func)
        self.assertEqual(tx2({"b": 2}, 3), 6)

    def test_str_transformer(self):
        """Tests a string Transformer."""
        txx = SimpleTransformer[None, str](lambda _, x: x + x)
        self.assertEqual(txx(None, "ab"), "abab")


class SinkTestCase(unittest.TestCase):
    """A test suite for Pythoid Sink classes."""

    def setUp(self) -> None:
        self.nres = -1
        self.sres = ""
        logging.basicConfig(
            format=LOG_MSG_FORMAT, datefmt=LOG_DATE_FORMAT, level=logging.WARNING
        )

    def set_nres(self, nres: int) -> str:
        """Set the value of an integer field (for side-effect testing)."""
        self.nres = nres
        return "ok"

    def set_sres(self, sres: str) -> bool:
        """Set the value of a string field (for side-effect testing)."""
        self.sres = sres
        return True

    def test_abstract_task(self):
        """Tests creating an abstract Task instance."""
        sink = Task()  # type: ignore
        self.assertRaises(NotImplementedError, sink.__call__, None)

    def test_abstract_stub(self):
        """Tests creating an abstract Stub instance."""
        sink = Stub()  # type: ignore
        self.assertRaises(NotImplementedError, sink.__call__, None, 0)

    def test_abstract_module(self):
        """Tests creating an abstract Module instance."""
        sink = Module({"a", "b"})  # type: ignore
        self.assertRaises(NotImplementedError, sink.__call__, None)

    def test_int_task(self):
        """Tests an integer Task."""
        func: Callable[[Any], Any] = lambda _: self.set_nres(5)
        sink: SimpleTask[Any, int] = SimpleTask(func)
        eff = sink(None)
        self.assertEqual(eff, "ok")
        self.assertEqual(self.nres, 5)

    def test_str_task(self):
        """Tests a string Task."""
        sink = SimpleTask[StrCtx, str](lambda ctx: self.set_sres(ctx["a"]))
        eff = sink({"a": "xyz", "b": "abc"})
        self.assertEqual(eff, True)
        self.assertEqual(self.sres, "xyz")

    def test_int_stub(self):
        """Tests an integer Stub."""
        sink = SimpleStub[None, int](lambda _, x: self.set_nres(x * 2))
        eff = sink(None, 4)
        self.assertEqual(eff, "ok")
        self.assertEqual(self.nres, 8)

    def test_str_stub(self):
        """Tests a string Stub."""
        sink = SimpleStub[StrCtx, str](lambda ctx, x: self.set_sres(x + ctx["a"]))
        eff = sink({"a": "xyz", "b": "abc"}, "abb")
        self.assertEqual(eff, True)
        self.assertEqual(self.sres, "abbxyz")

    def test_int_module(self):
        """Tests an integer Module."""
        join = SimpleModule[IntCtx, int](
            {"value", "plus", "times"},
            lambda ctx, **kw: self.set_nres(
                ctx["a"] + (kw["value"] + kw["plus"]) * kw["times"]
            ),
        )
        self.assertSetEqual(join.input_names(), {"value", "times", "plus"})
        eff = join({"a": 2, "b": 3}, value=3, plus=5, times=2)
        self.assertEqual(eff, "ok")
        self.assertEqual(self.nres, 18)

    def test_str_module(self):
        """Tests a string Module."""
        func: Callable[[None, KwArg(str)], Any] = lambda _, **kw: self.set_sres(
            kw["a"] + "|" + kw["b"]
        )
        join = SimpleModule({"a", "b"}, func)
        self.assertSetEqual(join.input_names(), {"a", "b"})
        eff = join(None, a="hi", b="there")
        self.assertEqual(eff, True)
        self.assertEqual(self.sres, "hi|there")


class JoinTestCase(unittest.TestCase):
    """A test suite for Pythoid Join class."""

    def setUp(self) -> None:
        logging.basicConfig(
            format=LOG_MSG_FORMAT, datefmt=LOG_DATE_FORMAT, level=logging.WARNING
        )

    def test_abstract_join(self):
        """Tests creating an abstract Join instance."""
        join = Join({"a"})  # type: ignore
        self.assertRaises(NotImplementedError, join.__call__, None)

    def test_int_join(self):
        """Tests an integer Join."""
        join = SimpleJoin[IntCtx, int](
            {"value", "plus", "times"},
            lambda ctx, **kw: ctx["a"] + (kw["value"] + kw["plus"]) * kw["times"],
        )
        self.assertSetEqual(join.input_names(), {"value", "times", "plus"})
        self.assertEqual(join({"a": 2, "b": 3}, value=3, plus=5, times=2), 18)

    def test_str_join(self):
        """Tests a string Join."""
        func: Callable[[None, KwArg(str)], str] = (
            lambda _, **kw: kw["a"] + "|" + kw["b"]
        )
        join = SimpleJoin({"a", "b"}, func)
        self.assertSetEqual(join.input_names(), {"a", "b"})
        self.assertEqual(join(None, a="hi", b="there"), "hi|there")


# test classes for pipeline suite
class AplusBtimesCjoin(Join[IntCtx, int]):
    """A Join(3) with inputs labeled 'a', 'b', and 'c'; computes (a+b)*c."""

    def __init__(self):
        super().__init__({"a", "b", "c"})
        self.underlying = lambda _, **kw: (kw["a"] + kw["b"]) * kw["c"]

    def _do_call__(self, ctx: IntCtx, **args: int) -> int:
        return self.underlying(ctx, **args)


class AplusBtimesCmodule(Module[IntCtx, int]):
    """
    A SinkJ(3) with inputs labeled 'a', 'b', and 'c';
    computes (a+b)*c and writes it to a field.
    """

    def set_result(self, nres: int) -> str:
        """Set the value of an integer field (for side-effect testing)."""
        self.result = nres
        return "ok"

    def __init__(self):
        super().__init__({"a", "b", "c"})
        self.result = 0
        self.underlying = lambda _, **kw: self.set_result((kw["a"] + kw["b"]) * kw["c"])

    def _do_call__(self, ctx: IntCtx, **args: int) -> str:
        return self.underlying(ctx, **args)


class PipelineTestCase(unittest.TestCase):
    """A test suite for Pythoid pipelines."""

    def setUp(self) -> None:
        self.nres = -1
        logging.basicConfig(
            format=LOG_MSG_FORMAT, datefmt=LOG_DATE_FORMAT, level=logging.WARNING
        )

    def set_nres(self, nres: int) -> str:
        """Set the value of an integer field (for side-effect testing)."""
        self.nres = nres
        return "ok"

    def test_source_transformer_chain(self):
        """Tests Source to Transformer pipeline."""
        src = SimpleSource[IntCtx, int](lambda ctx: ctx["a"])
        times3 = SimpleTransformer[IntCtx, int](lambda _, x: x * 3)
        plus5 = SimpleTransformer[IntCtx, int](lambda _, x: x + 5)
        pipe = src >> times3 >> plus5
        self.assertIsInstance(pipe, Source)
        self.assertEqual(pipe({"a": 22}), 71)

    def test_source_join_chain(self):
        """Tests Source to Join pipeline."""
        calc = AplusBtimesCjoin()
        srca = SimpleSource[Any, int](lambda _: 5)
        srcb = SimpleSource[Any, int](lambda _: 2)
        srcc = SimpleSource[Any, int](lambda _: 3)
        pipe = srcc >= (srcb >= (srca >= (calc, "a"), "b"), "c")
        self.assertIsInstance(pipe, Join)
        self.assertSetEqual(pipe.input_names(), set())
        self.assertEqual(pipe(None), 21)

    def test_source_stub_chain(self):
        """Tests Source to Stub pipeline."""
        src = SimpleSource[IntCtx, int](lambda ctx: ctx["a"])
        sink = SimpleStub[IntCtx, int](lambda _, x: self.set_nres(x * 3))
        pipe = src > sink
        self.assertIsInstance(pipe, Task)
        eff = pipe({"a": 5})
        self.assertEqual(eff, "ok")
        self.assertEqual(self.nres, 15)

    def test_source_module_chain(self):
        """Tests Source to Module pipeline."""
        calc = AplusBtimesCmodule()
        srca = SimpleSource[Any, int](lambda _: 5)
        srcc = SimpleSource[Any, int](lambda _: 3)
        pipe = srcc.to_module(srca.to_module(calc, "a"), "c")
        self.assertIsInstance(pipe, Module)
        self.assertSetEqual(pipe.input_names(), {"b"})
        eff = pipe(None, b=2)
        self.assertEqual(eff, "ok")
        self.assertEqual(calc.result, 21)

    def test_transformer_chain(self):
        """Tests Transformer chain."""
        dev2 = SimpleTransformer[Any, int](lambda _, x: int(x / 2))
        plus3 = SimpleTransformer[Any, int](lambda _, x: x + 3)
        times5 = SimpleTransformer[Any, int](lambda _, x: x * 5)
        pipe = dev2 >> plus3 >> times5
        self.assertIsInstance(pipe, Transformer)
        self.assertEqual(pipe(None, 8), 35)

    def test_transformer_stub_chain(self):
        """Tests Transformer to Stub pipeline."""
        div2 = SimpleTransformer[Any, int](lambda _, x: int(x / 2))
        sink = SimpleStub[Any, int](lambda _, x: self.set_nres(x * 3))
        pipe = div2 > sink
        self.assertIsInstance(pipe, Stub)
        eff = pipe(None, 8)
        self.assertEqual(eff, "ok")
        self.assertEqual(self.nres, 12)

    def test_transformer_module_chain(self):
        """Tests Transformer to Module pipeline."""
        calc = AplusBtimesCmodule()
        txa = SimpleTransformer[Any, int](lambda _, x: x * 2)
        txc = SimpleTransformer[Any, int](lambda _, x: x - 3)
        pipe = txc | (txa | (calc, "a", "ta"), "c", "tc")
        self.assertIsInstance(pipe, Module)
        self.assertSetEqual(pipe.input_names(), {"ta", "b", "tc"})
        eff = pipe(None, ta=2, b=3, tc=5)
        self.assertEqual(eff, "ok")
        self.assertEqual(calc.result, 14)

    def test_source_transformer_stub_chain(self):
        """Tests Source to Transformer to Stub pipeline."""
        src = SimpleSource[IntCtx, int](lambda ctx: ctx["a"])
        div2 = SimpleTransformer[IntCtx, int](lambda _, x: int(x / 2))
        sink = SimpleStub[IntCtx, int](lambda _, x: self.set_nres(x * 3))
        pipe = src >> div2 > sink
        self.assertIsInstance(pipe, Task)
        eff = pipe({"a": 8})
        self.assertEqual(eff, "ok")
        self.assertEqual(self.nres, 12)

    def test_join_transformer_chain(self):
        """Tests Join to Transformer pipeline."""
        join = AplusBtimesCjoin()
        div2 = SimpleTransformer[IntCtx, int](lambda _, x: int(x / 2))
        pipe = join >> div2
        self.assertIsInstance(pipe, Join)
        self.assertSetEqual(pipe.input_names(), {"a", "b", "c"})
        self.assertEqual(pipe({}, a=3, b=5, c=2), 8)

    def test_join_stub_chain(self):
        """Tests Join to Stub pipeline."""
        join = AplusBtimesCjoin()
        sink = SimpleStub[IntCtx, int](lambda _, x: self.set_nres(x * 3))
        pipe = join > sink
        self.assertIsInstance(pipe, Module)
        self.assertSetEqual(pipe.input_names(), {"a", "b", "c"})
        eff = pipe({}, a=3, b=5, c=2)
        self.assertEqual(eff, "ok")
        self.assertEqual(self.nres, 48)

    def test_join_module_chain(self):
        """Tests Join to Module pipeline."""
        join1 = AplusBtimesCjoin()
        join2 = AplusBtimesCmodule()
        pipe = join1 | (join2, "b", {"a": "d", "b": "e", "c": "f"})
        self.assertIsInstance(pipe, Module)
        self.assertSetEqual(pipe.input_names(), {"a", "c", "d", "e", "f"})
        eff = pipe({}, a=1, c=2, d=3, e=4, f=5)
        self.assertEqual(eff, "ok")
        self.assertEqual(join2.result, 72)

    def test_partial_source_join_chain(self):
        """Tests partial Source to Join pipeline."""
        calc = AplusBtimesCjoin()
        srca = SimpleSource[Any, int](lambda _: 5)
        srcc = SimpleSource[Any, int](lambda _: 3)
        pipe = srcc.to_join(srca.to_join(calc, "a"), "c")
        self.assertIsInstance(pipe, Join)
        self.assertSetEqual(pipe.input_names(), {"b"})
        self.assertEqual(pipe(None, b=4), 27)

    def test_transform_join_chain_same_name(self):
        """Tests Transformer to Join pipeline without name change."""
        calc = AplusBtimesCjoin()
        txa = SimpleTransformer[Any, int](lambda _, x: x + 1)
        txc = SimpleTransformer[Any, int](lambda _, x: x - 1)
        pipe = txc >= (txa >= (calc, "a", ""), "c", "")
        self.assertIsInstance(pipe, Join)
        self.assertSetEqual(pipe.input_names(), {"a", "b", "c"})
        self.assertEqual(pipe(None, a=2, b=3, c=4), 18)

    def test_transform_join_chain_new_name(self):
        """Tests Transformer to Join pipeline with name change."""
        calc = AplusBtimesCjoin()
        txa = SimpleTransformer[Any, int](lambda _, x: x * 2)
        txc = SimpleTransformer[Any, int](lambda _, x: x - 3)
        pipe = txc >= (txa >= (calc, "a", "ta"), "c", "tc")
        self.assertIsInstance(pipe, Join)
        self.assertSetEqual(pipe.input_names(), {"ta", "b", "tc"})
        self.assertEqual(pipe(None, ta=2, b=3, tc=5), 14)

    def test_join_join_chain_no_remap(self):
        """Tests Join to Join pipeline without name remap."""
        join1 = AplusBtimesCjoin()
        join2 = SimpleJoin[Any, int](
            {"d", "e", "f"}, lambda _, **kw: kw["d"] + kw["e"] - kw["f"]
        )
        pipe = join1 >= (join2, "d", {})
        self.assertIsInstance(pipe, Join)
        self.assertSetEqual(pipe.input_names(), {"a", "b", "c", "e", "f"})
        self.assertEqual(pipe({}, a=1, b=2, c=4, e=8, f=16), 4)

    def test_join_join_chain_with_remap(self):
        """Tests Join to Join pipeline with name remap."""
        join1 = AplusBtimesCjoin()
        join2 = SimpleJoin[Any, int](
            {"d", "e", "f"}, lambda _, **kw: kw["d"] + kw["e"] - kw["f"]
        )
        pipe = join1 >= (join2, "d", {"a": "aa", "c": "cc"})
        self.assertIsInstance(pipe, Join)
        self.assertSetEqual(pipe.input_names(), {"aa", "b", "cc", "e", "f"})
        self.assertEqual(pipe({}, aa=1, b=2, cc=4, e=8, f=16), 4)

    def test_valid_input_name(self):
        """Tests connecting nodes to non-existing inputs."""
        src = SimpleSource[IntCtx, int](lambda ctx: ctx["a"])
        tx = SimpleTransformer[Any, int](lambda _, x: x * 2)
        join = AplusBtimesCjoin()
        mod = AplusBtimesCmodule()

        self.assertRaises(AssertionError, lambda: src.to_join(join, "?"))
        self.assertRaises(AssertionError, lambda: src.to_module(mod, "?"))
        self.assertRaises(AssertionError, lambda: tx.to_join(join, "?"))
        self.assertRaises(AssertionError, lambda: tx.to_module(mod, "?"))
        self.assertRaises(AssertionError, lambda: join.to_join(join, "?"))
        self.assertRaises(AssertionError, lambda: join.to_module(mod, "?"))

    def test_valid_remap_key(self):
        """Tests connecting nodes with non-existing inputs remap keys."""
        join = AplusBtimesCjoin()
        mod = AplusBtimesCmodule()
        self.assertRaises(AssertionError, lambda: join.to_join(join, "a", {"?": "aa"}))
        self.assertRaises(AssertionError, lambda: join.to_module(mod, "a", {"?": "aa"}))


if __name__ == "__main__":
    unittest.main()
