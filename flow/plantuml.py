"""Module providing PlantUML import and export capabilities for Pythoid flows."""
import importlib
import inspect
import json
from dataclasses import dataclass
from enum import EnumMeta
from pathlib import Path
from typing import Any, Dict, Generic, List, Type

from .builder import Connection, FlowBuilder
from .common import CTX, Node, T


@dataclass(frozen=True)
class NodeDef:
    """Node definition as read from PlantUML JSON nodes section."""

    class_name: str
    params: Dict[str, Any]

    @classmethod
    def from_dict(cls, data: Dict):
        """
        Creates an instance of NodeDef from a dictionary;
        it needs to contain "class" key.
        """
        class_name = data.pop("class")
        return NodeDef(class_name, data)


class NodeFactory(Generic[CTX, T]):
    """Factory for creating random nodes."""

    def __init__(self) -> None:
        self.modules: List[Any] = []

    def add_module(self, name: str) -> None:
        """Imports a module containing node class definitions."""
        self.modules.append(importlib.import_module(name))

    def create_node(self, node_def: NodeDef) -> Node[CTX, T]:
        """Creates a node from the definition."""
        cls = self.get_class(node_def.class_name)
        params = {
            key: NodeFactory._cast_value(
                node_def.params[key]
                if val.default == val.empty
                else node_def.params.get(key, val.default),
                val,
            )
            for key, val in inspect.signature(cls).parameters.items()
        }
        return cls(**params)

    def create_nodes(self, data: Dict) -> Dict[str, Node[CTX, T]]:
        """Creates nodes from their definitions."""
        node_defs = dict((k, NodeDef.from_dict(v)) for k, v in data.items())
        return dict((k, self.create_node(nd)) for k, nd in node_defs.items())

    @staticmethod
    def _cast_value(value: Any, param: inspect.Parameter) -> Any:
        if isinstance(param.annotation, EnumMeta):
            emeta: EnumMeta = param.annotation
            return emeta(value)
        return value

    def get_class(self, class_name: str):
        """Returns a Class object for the given name."""
        # try loading a global class
        try:
            return globals()[class_name]
        except KeyError:
            pass
        # try loading class from a module
        for mod in self.modules:
            try:
                return getattr(mod, class_name)
            except AttributeError:
                pass
        # if class could not be found, error out
        raise NameError(f"No module found for class {class_name}")


class PlantumlFlowBuilder(Generic[CTX, T]):
    """Builds a flow from a PlantUML file."""

    def __init__(self, lines: List[str]) -> None:
        self.lines = list(filter(lambda ln: not ln.startswith("'"), lines))
        self.node_factory = NodeFactory[CTX, T]()

    def add_module(self, name: str):
        """Imports the specified module."""
        self.node_factory.add_module(name)

    def build_flow(self) -> Node[CTX, T]:
        """Builds a flow."""
        json_data = self._parse_nodes_json()
        node_defs = dict((k, NodeDef.from_dict(v)) for k, v in json_data.items())
        nodes_by_alias = dict(
            (k, self.node_factory.create_node(nd)) for k, nd in node_defs.items()
        )

        connections = self._parse_connections(nodes_by_alias)

        builder = FlowBuilder[CTX, T]()
        for conn in connections:
            builder.add_connection(conn)

        return builder.build()

    @classmethod
    def from_file(
        cls: Type["PlantumlFlowBuilder[CTX, T]"], path: Path
    ) -> "PlantumlFlowBuilder[CTX, T]":
        """Creates an instance of PlantumlFlowBuilder from a PlantUML file."""
        with open(path, mode="r", encoding="UTF-8") as in_file:
            lines = in_file.readlines()
        return PlantumlFlowBuilder(lines)

    def _parse_connections(self, nodes_by_alias: Dict[str, Node]):

        # connection token : left-to-right is Origin-to-Target?
        tokens = {
            "---->": True,
            "--->": True,
            "-->": True,
            "->": True,
            "<----": False,
            "<---": False,
            "<--": False,
            "<-": False,
        }

        def get_origin(label: str):
            org_name = label.strip()
            return nodes_by_alias[org_name]

        def get_target_and_port(label: str):
            tgt_name = label.strip()
            if tgt_name.find("_") > 0:
                tgt_parts = tgt_name.split("_")
                target = nodes_by_alias[tgt_parts[0]]
                port = tgt_parts[1]
            else:
                target = nodes_by_alias[tgt_name]
                port = None
            return target, port

        def create_connection(one_line: str):
            for token, ltr in tokens.items():
                if one_line.find(token) > 0:
                    parts = one_line.split(token)
                    if not ltr:
                        parts.reverse()
                    origin = get_origin(parts[0])
                    target, port = get_target_and_port(parts[1])
                    conn = Connection(origin=origin, target=target)
                    if port:
                        conn.as_input(port)
                    conn.validate()
                    return conn
            return None

        return list(
            filter(
                lambda c: c is not None, [create_connection(ln) for ln in self.lines]
            )
        )

    def _parse_nodes_json(self) -> Dict:
        prefix = "json nodes"
        state = "out"
        braces = 0
        buffer = []

        def process_line(one_line: str) -> bool:
            nonlocal braces
            buffer.append(one_line)
            braces += line.count("{") - line.count("}")
            return braces == 0

        for line in self.lines:
            match state:
                case "out" if line.find(prefix) >= 0:
                    state = "in"
                    if process_line(line):
                        break
                case "in":
                    if process_line(line):
                        break

        if len(buffer) == 0:
            raise ValueError("'nodes' JSON not found or invalid")

        buf = "".join(buffer)[len(prefix) :]
        return json.loads(buf)
