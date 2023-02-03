"""Module providing unit tests for PlantUML builder."""
import json
import logging
import unittest
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, cast

from flow import Join, Source
from flow.plantuml import NodeDef, NodeFactory, PlantumlFlowBuilder
from tests import LOG_DATE_FORMAT, LOG_MSG_FORMAT, data_filepath

IntCtx = Dict[str, int]


class NodeDefTestCase(unittest.TestCase):
    """A test suite for NodeDef class."""

    def test_from_dict(self):
        """Tests creating a NodeDef from a dictionary."""
        data = {"class": "a.b.c.TestNode", "p1": 5, "p2": "abc", "p3": True}
        node_def = NodeDef.from_dict(data)
        self.assertEqual(node_def.class_name, "a.b.c.TestNode")
        self.assertDictEqual(node_def.params, {"p1": 5, "p2": "abc", "p3": True})


class Color(str, Enum):
    """Test enum class."""

    RED = "red"
    GREEN = "green"
    BLUE = "blue"


@dataclass
class TestNode(Source[Any, Any]):
    """Test node class."""

    active: bool
    color: Color
    tags: List[str]
    rate: int = 24
    label: str = "n/a"

    def _do_call__(self, ctx: Any) -> Any:
        return None


class NodeFactoryTestCase(unittest.TestCase):
    """A test suite for NodeFactory."""

    def test_get_class(self):
        """Tests factory get_class method."""
        factory = NodeFactory[Any, Any]()
        factory.add_module(__name__)
        cls = factory.get_class("TestNode")
        self.assertEqual(cls, TestNode)

    def test_create_node(self):
        """Tests factory create_node method."""
        node_def = NodeDef(
            class_name="TestNode",
            params={"active": True, "color": "green", "tags": ["x", "y"]},
        )
        factory = NodeFactory[Any, Any]()
        factory.add_module(__name__)
        node = factory.create_node(node_def)
        self.assertIsInstance(node, TestNode)
        test_node = cast(TestNode, node)
        self.assertTrue(test_node.active)
        self.assertIsInstance(test_node.color, Color)
        self.assertEqual(test_node.color, Color.GREEN)
        self.assertIsInstance(test_node.tags, List)
        self.assertListEqual(test_node.tags, ["x", "y"])
        self.assertEqual(test_node.rate, 24)
        self.assertEqual(test_node.label, "n/a")

    def test_create_nodes(self):
        """Tests creating various nodes from JSON."""
        path = data_filepath("puml/nodes.json")
        with open(path, mode="r", encoding="UTF-8") as in_file:
            lines = in_file.read()
        factory = NodeFactory[Any, Any]()
        factory.add_module("tests.custom_nodes1")
        factory.add_module("tests.custom_nodes2")
        nodes = factory.create_nodes(json.loads(lines))
        self.assertSetEqual(
            set(nodes.keys()),
            {"srcA", "srcB", "srcC", "times2", "plus3", "minus1", "rem3", "j1", "j2"},
        )
        self.assertEqual(nodes["srcA"].key, "a")  # type: ignore
        self.assertEqual(nodes["srcB"].key, "b")  # type: ignore
        self.assertEqual(nodes["srcC"].key, "c")  # type: ignore
        self.assertEqual(nodes["times2"].value, 2)  # type: ignore
        self.assertEqual(nodes["plus3"].value, 3)  # type: ignore
        self.assertEqual(nodes["minus1"].value, -1)  # type: ignore
        self.assertEqual(nodes["rem3"].value, 3)  # type: ignore
        self.assertEqual(nodes["j1"].input_names(), {"u", "w"})  # type: ignore
        self.assertEqual(nodes["j1"].expression, "u * w")  # type: ignore
        self.assertEqual(nodes["j2"].input_names(), {"x", "y", "z"})  # type: ignore
        self.assertEqual(nodes["j2"].expression, "x + y - z")  # type: ignore


class PlantumlFlowBuilderTestCase(unittest.TestCase):
    """A test suite for PlantumlFlowBuilder."""

    path: Path

    @classmethod
    def setUpClass(cls) -> None:
        cls.path = data_filepath("puml/flow1.puml")
        logging.basicConfig(
            format=LOG_MSG_FORMAT, datefmt=LOG_DATE_FORMAT, level=logging.WARNING
        )

    def test_build_flow(self):
        """Tests creating a flow from PlantUML file."""
        builder = PlantumlFlowBuilder[IntCtx, int].from_file(self.path)
        builder.add_module("tests.custom_nodes1")
        builder.add_module("tests.custom_nodes2")
        flow = builder.build_flow()
        self.assertIsInstance(flow, Join)
        self.assertSetEqual(flow.input_names(), {"w"})  # type: ignore
        self.assertEqual(flow({"a": 4, "b": 3, "c": 5}, w=3), 11)  # type: ignore
