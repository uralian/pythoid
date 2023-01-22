"""Module providing unit tests for Pythoid Flow Builder."""
import logging
import unittest
from typing import Dict, cast

from flow import (
    Join,
    Module,
    SimpleJoin,
    SimpleSource,
    SimpleStub,
    SimpleTask,
    SimpleTransformer,
    Source,
    Stub,
    Task,
    Transformer,
)
from flow.builder import Connection, FlowBuilder
from tests import LOG_DATE_FORMAT, LOG_MSG_FORMAT

IntCtx = Dict[str, int]


class ConnectionTestCase(unittest.TestCase):
    """A test suite for Connection class."""

    def test_setters(self):
        """Tests Connection set_ methods."""
        src_a = SimpleSource[IntCtx, int](lambda ctx: ctx["a"])
        src_b = SimpleSource[IntCtx, int](lambda ctx: ctx["b"])
        con = Connection(origin=src_a, target=src_b)
        con.set_origin(src_b)
        con.set_target(src_a)
        con.as_input("port1")
        con.rename_input("port2")
        con.remap_inputs({"x": "y"})
        self.assertEqual(con.origin, src_b)
        self.assertEqual(con.target, src_a)
        self.assertEqual(con.input_name, "port1")
        self.assertEqual(con.new_input_name, "port2")
        self.assertEqual(con.inputs_remap, {"x": "y"})

    def test_valid_origin(self):
        """Tests origin validation."""
        con = Connection(
            origin=SimpleTask(lambda ctx: None),
            target=SimpleTransformer(lambda _, x: x + 1),
        )
        self.assertRaises(AssertionError, con.validate)
        con.set_origin(SimpleStub(lambda _, x: None))
        self.assertRaises(AssertionError, con.validate)

    def test_valid_target(self):
        """Tests target validation."""
        con = Connection(
            origin=SimpleSource(lambda ctx: 5), target=SimpleSource(lambda ctx: 5)
        )
        self.assertRaises(AssertionError, con.validate)
        con.set_target(SimpleTask(lambda _: None))
        self.assertRaises(AssertionError, con.validate)

    def test_input_name(self):
        """Tests input name validation."""
        con = Connection(
            origin=SimpleSource(lambda ctx: 5),
            target=SimpleTransformer(lambda ctx, x: x * 2),
            input_name="a",
        )
        self.assertRaises(AssertionError, con.validate)
        con.set_target(SimpleJoin({"b"}, lambda **kw: 3))
        self.assertRaises(AssertionError, con.validate)
        con.set_target(SimpleJoin({"a"}, lambda **kw: 3))
        con.validate()

    def test_new_input_name(self):
        """Tests new input name validation."""
        con = Connection(
            origin=SimpleSource(lambda ctx: 5),
            target=SimpleJoin({"a"}, lambda **kw: 3),
            input_name="a",
            new_input_name="aa",
        )
        self.assertRaises(AssertionError, con.validate)
        con.set_origin(SimpleTransformer(lambda ctx, x: x * 2))
        con.validate()

    def test_inputs_remap(self):
        """Tests inputs remap validation."""
        con = Connection(
            origin=SimpleSource(lambda ctx: 5),
            target=SimpleJoin({"a"}, lambda **kw: 3),
            input_name="a",
            inputs_remap={"a": "aa"},
        )
        self.assertRaises(AssertionError, con.validate)
        con.set_origin(SimpleJoin({"a"}, lambda **kw: 3))
        con.validate()


class FlowBuilderTestCase(unittest.TestCase):
    """A test suite for Flow Builder."""

    src_a: Source[IntCtx, int]
    src_b: Source[IntCtx, int]
    src_c: Source[IntCtx, int]
    times2: Transformer[IntCtx, int]
    plus3: Transformer[IntCtx, int]
    minus1: Transformer[IntCtx, int]
    square: Transformer[IntCtx, int]
    rem3: Transformer[IntCtx, int]
    x_plus_y_minus_z: Join[IntCtx, int]
    u_times_w: Join[IntCtx, int]
    save: Stub[IntCtx, int]
    result: int

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        logging.basicConfig(
            format=LOG_MSG_FORMAT, datefmt=LOG_DATE_FORMAT, level=logging.WARNING
        )
        cls._init_nodes()

    @classmethod
    def _init_nodes(cls):
        cls.src_a = SimpleSource[IntCtx, int](lambda ctx: ctx["a"])
        cls.src_b = SimpleSource[IntCtx, int](lambda ctx: ctx["b"])
        cls.src_c = SimpleSource[IntCtx, int](lambda ctx: ctx["c"])
        cls.times2 = SimpleTransformer[IntCtx, int](lambda _, x: x * 2)
        cls.plus3 = SimpleTransformer[IntCtx, int](lambda _, x: x + 3)
        cls.minus1 = SimpleTransformer[IntCtx, int](lambda _, x: x - 1)
        cls.square = SimpleTransformer[IntCtx, int](lambda _, x: x * x)
        cls.rem3 = SimpleTransformer[IntCtx, int](lambda _, x: x % 3)
        cls.x_plus_y_minus_z = SimpleJoin[IntCtx, int](
            {"x", "y", "z"}, lambda _, **kw: kw["x"] + kw["y"] - kw["z"]
        )
        cls.u_times_w = SimpleJoin[IntCtx, int](
            {"u", "w"}, lambda _, **kw: kw["u"] * kw["w"]
        )
        cls.save = SimpleStub(lambda _, x: cls.save_result(x))

    @classmethod
    def save_result(cls, result: int) -> bool:
        """Saves result into a class variable."""
        cls.result = result
        return True

    def test_simple_flow(self):
        """Tests simple source->transformer->transformer->... pipeline."""
        builder = FlowBuilder[IntCtx, int]()
        builder.connect(self.src_a).to(self.times2)
        builder.connect(self.times2).to(self.plus3)
        builder.connect(self.plus3).to(self.square)
        flow = builder.build()
        self.assertIsInstance(flow, Source)
        source = cast(Source[IntCtx, int], flow)
        self.assertEqual(source({"a": 5}), 169)
        self.assertEqual(source({"a": 2}), 49)

    def test_task(self):
        """Tests a flow representing a Task."""
        builder = FlowBuilder[IntCtx, int]()
        builder.connect(self.src_a).to(self.plus3)
        builder.connect(self.plus3).to(self.times2)
        builder.connect(self.times2).to(self.save)
        flow = builder.build()
        self.assertIsInstance(flow, Task)
        task = cast(Task[IntCtx, int], flow)
        self.assertTrue(task({"a": 5}))
        self.assertEqual(self.result, 16)
        self.assertTrue(task({"a": -1}))
        self.assertEqual(self.result, 4)

    def test_join_flow(self):
        """Tests a flow with a Join."""
        builder = FlowBuilder[IntCtx, int]()
        builder.connect(self.plus3).to_input(self.x_plus_y_minus_z, "x").rename_input(
            "xx"
        )
        builder.connect(self.times2).to_input(self.x_plus_y_minus_z, "z").rename_input(
            "zz"
        )
        builder.connect(self.src_a).to_input(self.x_plus_y_minus_z, "y")
        builder.connect(self.x_plus_y_minus_z).to(self.square)
        flow = builder.build()
        self.assertIsInstance(flow, Join)
        join = cast(Join[IntCtx, int], flow)
        self.assertSetEqual(join.input_names(), {"xx", "zz"})
        self.assertEqual(join({"a": 2}, xx=3, zz=4), 0)
        self.assertEqual(join({"a": 1}, xx=5, zz=2), 25)

    def test_complex_flow(self):
        """Tests a complex flow with multiple joins."""
        # a --> minus1 ----------------\
        #                                > (x)
        # b -------> (u) u_times_w ------> (y) x_plus_y_minus_z --> plus3 --> save
        #          > (w)                 > (z)
        # (ww) ---/                    /
        # c --> rem3 --> times2 ------/
        builder = FlowBuilder[IntCtx, int]()
        builder.connect(self.src_a).to(self.minus1)
        builder.connect(self.src_b).to_input(self.u_times_w, "u")
        builder.connect(self.src_c).to(self.rem3)
        builder.connect(self.rem3).to(self.times2)
        builder.connect(self.minus1).to(self.x_plus_y_minus_z).as_input("x")
        builder.connect(self.u_times_w).to_input(
            self.x_plus_y_minus_z, "y"
        ).remap_inputs({"w": "ww", "u": "uu"})
        builder.connect(self.times2).to(self.x_plus_y_minus_z).as_input(
            "z"
        ).rename_input("zz")
        builder.connect(self.x_plus_y_minus_z).to(self.plus3)
        builder.connect(self.plus3).to(self.save)
        flow = builder.build()
        self.assertIsInstance(flow, Module)
        module = cast(Module[IntCtx, int], flow)
        self.assertSetEqual(module.input_names(), {"ww"})
        self.assertTrue(module({"a": 4, "b": 3, "c": 5}, ww=3))
        self.assertEqual(self.result, 11)
        self.assertTrue(module({"a": 3, "b": 2, "c": 4}, ww=5))
        self.assertEqual(self.result, 13)


if __name__ == "__main__":
    unittest.main()
