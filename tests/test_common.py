import unittest
from typing import Any, Callable, Dict

from mypy_extensions import KwArg

from flow import SimpleSource, SimpleTransformer, SimpleSink, SimpleJoin
from flow.common import add_set_items, remove_set_items, add_dict_entry

IntCtx = Dict[str, int]
StrCtx = Dict[str, str]


class CommonFunctionsTestCase(unittest.TestCase):

    def test_add_set_items(self):
        src = {"a", "b", "c"}
        tgt = add_set_items(src, "x", "b", "z")
        self.assertSetEqual(src, {"a", "b", "c"})
        self.assertSetEqual(tgt, {"a", "b", "c", "x", "z"})

    def test_remove_set_items(self):
        src = {"a", "b", "c", "d", "e"}
        tgt = remove_set_items(src, "d", "a", "x", "y")
        self.assertSetEqual(src, {"a", "b", "c", "d", "e"})
        self.assertSetEqual(tgt, {"b", "c", "e"})

    def test_add_dict_entry(self):
        src = {"a": 1, "b": 2, "c": 3}
        tgt1 = add_dict_entry(src, "x", 5)
        tgt2 = add_dict_entry(tgt1, "a", 9)
        self.assertDictEqual(src, {"a": 1, "b": 2, "c": 3})
        self.assertDictEqual(tgt1, {"a": 1, "b": 2, "c": 3, "x": 5})
        self.assertDictEqual(tgt2, {"a": 9, "b": 2, "c": 3, "x": 5})


class SourceTestCase(unittest.TestCase):

    def test_int_source(self):
        src = SimpleSource[IntCtx, int](lambda ctx: ctx["a"])
        self.assertEqual(src({"a": 5, "b": 3}), 5)

    def test_str_source(self):
        func: Callable[[Any], str] = lambda _: "a"
        src = SimpleSource(func)
        self.assertEqual(src(None), "a")


class TransformerTestCase(unittest.TestCase):

    def test_int_transformer(self):
        func: Callable[[IntCtx, int], int] = lambda ctx, x: x * ctx["b"]
        tx2 = SimpleTransformer(func)
        self.assertEqual(tx2({"b": 2}, 3), 6)

    def test_str_transformer(self):
        txx = SimpleTransformer[None, str](lambda _, x: x + x)
        self.assertEqual(txx(None, "ab"), "abab")


class SinkTestCase(unittest.TestCase):

    def test_int_sink(self):
        func: Callable[[Any, int], None] = lambda _, x: setattr(self, "nres", x * 2)
        sink = SimpleSink(func)
        sink(None, 4)
        self.assertEqual(self.nres, 8)

    def test_str_sink(self):
        sink = SimpleSink[StrCtx, str](lambda ctx, x: setattr(self, "sres", x + ctx["a"]))
        sink({"a": "xyz", "b": "abc"}, "abb")
        self.assertEqual(self.sres, "abbxyz")


class JoinTestCase(unittest.TestCase):

    def test_int_join(self):
        join = SimpleJoin[IntCtx, int](
            {"value", "plus", "times"},
            lambda ctx, **kw: ctx["a"] + (kw["value"] + kw["plus"]) * kw["times"]
        )
        self.assertSetEqual(join.input_names(), {"value", "times", "plus"})
        self.assertEqual(join({"a": 2, "b": 3}, value=3, plus=5, times=2), 18)

    def test_str_join(self):
        func: Callable[[None, KwArg(str)], str] = lambda _, **kw: kw["a"] + "|" + kw["b"]
        join = SimpleJoin({"a", "b"}, func)
        self.assertSetEqual(join.input_names(), {"a", "b"})
        self.assertEqual(join(None, a="hi", b="there"), "hi|there")


class PipelineTestCase(unittest.TestCase):

    def test_source_transformer_chain(self):
        src = SimpleSource[IntCtx, int](lambda ctx: ctx["a"])
        times3 = SimpleTransformer[IntCtx, int](lambda _, x: x * 3)
        plus5 = SimpleTransformer[IntCtx, int](lambda _, x: x + 5)
        pipe = src >> times3 >> plus5
        self.assertEqual(pipe({"a": 22}), 71)

    def test_source_sink_chain(self):
        src = SimpleSource[IntCtx, int](lambda ctx: ctx["a"])
        sink = SimpleSink[IntCtx, int](lambda _, x: setattr(self, "nres", x * 3))
        pipe = src > sink
        pipe({"a": 5})
        self.assertEqual(self.nres, 15)

    def test_transformer_chain(self):
        dev2 = SimpleTransformer[None, int](lambda _, x: x / 2)
        plus3 = SimpleTransformer[None, int](lambda _, x: x + 3)
        times5 = SimpleTransformer[None, int](lambda _, x: x * 5)
        pipe = dev2 >> plus3 >> times5
        self.assertEqual(pipe(None, 8), 35)

    def test_transformer_sink_chain(self):
        div2 = SimpleTransformer[None, int](lambda _, x: x / 2)
        sink = SimpleSink[None, int](lambda _, x: setattr(self, "nres", x * 3))
        pipe = div2 > sink
        pipe(None, 8)
        self.assertEqual(self.nres, 12)

    def test_source_transformer_sink_chain(self):
        src = SimpleSource[IntCtx, int](lambda ctx: ctx["a"])
        div2 = SimpleTransformer[IntCtx, int](lambda _, x: x / 2)
        sink = SimpleSink[IntCtx, int](lambda _, x: setattr(self, "nres", x * 3))
        pipe = src >> div2 > sink
        pipe({"a": 8})
        self.assertEqual(self.nres, 12)

    def test_join_transformer_chain(self):
        join = SimpleJoin[None, int]({"a", "b", "c"}, lambda _, **kw: (kw["a"] + kw["b"]) * kw["c"])
        div2 = SimpleTransformer[None, int](lambda _, x: x / 2)
        pipe = join >> div2
        self.assertSetEqual(pipe.input_names(), {"a", "b", "c"})
        self.assertEqual(pipe(None, a=3, b=5, c=2), 8)

    def test_join_sink_chain(self):
        join = SimpleJoin[Any, int]({"a", "b", "c"}, lambda _, **kw: (kw["a"] + kw["b"]) * kw["c"])
        sink = SimpleSink[Any, int](lambda _, x: setattr(self, "nres", x * 3))
        pipe = join > sink
        pipe(None, a=3, b=5, c=2)
        self.assertSetEqual(pipe.input_names(), {"a", "b", "c"})
        self.assertEqual(self.nres, 48)

    def test_partial_source_join_chain(self):
        calc = SimpleJoin[Any, int]({"a", "b", "c"}, lambda _, **kw: (kw["a"] + kw["b"]) * kw["c"])
        srca = SimpleSource[Any, int](lambda _: 5)
        srcc = SimpleSource[Any, int](lambda _: 3)
        pipe = srcc.to_join(srca.to_join(calc, "a"), "c")
        self.assertSetEqual(pipe.input_names(), {"b"})
        self.assertEqual(pipe(None, b=4), 27)

    def test_full_source_join_chain(self):
        calc = SimpleJoin[Any, int]({"a", "b", "c"}, lambda _, **kw: (kw["a"] + kw["b"]) * kw["c"])
        srca = SimpleSource[Any, int](lambda _: 5)
        srcb = SimpleSource[Any, int](lambda _: 2)
        srcc = SimpleSource[Any, int](lambda _: 3)
        pipe = srcc >= (srcb >= (srca >= (calc, "a"), "b"), "c")
        self.assertSetEqual(pipe.input_names(), set())
        self.assertEqual(pipe(None), 21)

    def test_transform_join_chain_same_name(self):
        calc = SimpleJoin[Any, int]({"a", "b", "c"}, lambda _, **kw: kw["a"] * kw["b"] * kw["c"])
        txa = SimpleTransformer[Any, int](lambda _, x: x + 1)
        txc = SimpleTransformer[Any, int](lambda _, x: x - 1)
        pipe = txc >= (txa >= (calc, "a"), "c")
        self.assertSetEqual(pipe.input_names(), {"a", "b", "c"})
        self.assertEqual(pipe(None, a=2, b=3, c=4), 27)

    def test_transform_join_chain_new_name(self):
        calc = SimpleJoin[Any, int]({"a", "b", "c"}, lambda _, **kw: kw["a"] * kw["b"] * kw["c"])
        txa = SimpleTransformer[Any, int](lambda _, x: x * 2)
        txc = SimpleTransformer[Any, int](lambda _, x: x - 3)
        pipe = txc >= (txa >= (calc, "a", "ta"), "c", "tc")
        self.assertSetEqual(pipe.input_names(), {"ta", "b", "tc"})
        self.assertEqual(pipe(None, ta=2, b=3, tc=5), 24)

    def test_join_join_chain_no_remap(self):
        j1 = SimpleJoin[Any, int]({"a", "b", "c"}, lambda _, **kw: kw["a"] * kw["b"] * kw["c"])
        j2 = SimpleJoin[Any, int]({"d", "e", "f"}, lambda _, **kw: kw["d"] + kw["e"] - kw["f"])
        pipe = j1 >= (j2, "d")
        self.assertSetEqual(pipe.input_names(), {"a", "b", "c", "e", "f"})
        self.assertEqual(pipe(None, a=1, b=2, c=4, e=8, f=16), 0)

    def test_join_join_chain_with_remap(self):
        j1 = SimpleJoin[Any, int]({"a", "b", "c"}, lambda _, **kw: kw["a"] * kw["b"] * kw["c"])
        j2 = SimpleJoin[Any, int]({"d", "e", "f"}, lambda _, **kw: kw["d"] + kw["e"] - kw["f"])
        pipe = j1 >= (j2, "d", {"a": "aa", "c": "cc"})
        self.assertSetEqual(pipe.input_names(), {"aa", "b", "cc", "e", "f"})
        self.assertEqual(pipe(None, aa=1, b=2, cc=4, e=8, f=16), 0)


if __name__ == '__main__':
    unittest.main()
