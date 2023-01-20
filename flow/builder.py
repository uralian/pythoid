"""Module providing Builder interface to compose Pythoid dataflows."""
import logging
from dataclasses import dataclass
from typing import Dict, Generic, List, Optional, Set

from .common import (
    CTX,
    Join,
    Module,
    MultiInput,
    Node,
    NoInput,
    SingleInput,
    Source,
    Stub,
    T,
    Task,
    Transformer,
)


@dataclass(kw_only=True)
class Connection(Generic[CTX, T]):
    """Represents a connection between two nodes."""

    src: Node[CTX, T]
    tgt: Node[CTX, T]
    input_name: Optional[str] = None
    new_input_name: Optional[str] = None
    inputs_remap: Optional[Dict[str, str]] = None

    def __post_init__(self):
        self._validate()

    def set_input(self, name: str) -> "Connection":
        """Sets the input name when the target node is MultiInput"""
        object.__setattr__(self, "input_name", name)
        self._validate()
        return self

    def rename_input(self, new_name: str) -> "Connection":
        """Renames the input when the target node is MultiInput and source node is Transformer"""
        object.__setattr__(self, "new_input_name", new_name)
        self._validate()
        return self

    def remap_inputs(self, remap: Dict[str, str]) -> "Connection":
        """Relabel source's inputs when the target node is MultiInput and surce node is Join"""
        object.__setattr__(self, "inputs_remap", remap)
        self._validate()
        return self

    def _validate(self):
        self._assert_source_has_output()
        self._assert_target_has_inputs()
        self._assert_input_name()
        self._assert_input_rename()
        self._assert_input_remap()

    def _assert_source_has_output(self):
        assert not isinstance(self.src, Task), "Source node cannot be a Task"
        assert not isinstance(self.src, Stub), "Source node cannot be a Stub"
        assert not isinstance(self.src, Module), "Source node cannot be a Module"

    def _assert_target_has_inputs(self):
        assert not isinstance(
            self.tgt, NoInput
        ), "Target node cannot be a no-input node"

    def _assert_input_name(self):
        if isinstance(self.tgt, MultiInput):
            assert (
                self.input_name
            ), "Input name must be specified connecting to a multi-input node"
            assert self.input_name in self.tgt.input_names()
        else:
            assert (
                not self.input_name
            ), "Input name cannot be specified connecting to a single-input node"

    def _assert_input_rename(self):
        if self.new_input_name:
            assert isinstance(
                self.tgt, MultiInput
            ), "Cannot rename input: target node is not a multi-input node"
            assert isinstance(
                self.src, Transformer
            ), "Cannot rename input: source node is not a Transformer"

    def _assert_input_remap(self):
        if self.inputs_remap:
            assert isinstance(
                self.tgt, MultiInput
            ), "Cannot rename input: target node is not a multi-input node"
            assert isinstance(
                self.src, Join
            ), "Cannot remap inputs: source node is not a Join"


@dataclass(frozen=True)
class Connector(Generic[CTX, T]):
    """Helper class for providing fluent builder syntax."""

    builder: "FlowBuilder[CTX, T]"
    src: Node[CTX, T]

    def to(self, tgt: SingleInput[CTX, T]):
        """Connects to a SingleInput node."""
        conn = Connection(src=self.src, tgt=tgt)
        self.builder.connections.append(conn)
        self.builder.log.info("src=%s, tgt=%s", self.src, tgt)
        return conn

    def to_input(
        self,
        tgt: MultiInput[CTX, T],
        input_name: str,
        *,
        new_input_name: Optional[str] = None,
        inputs_remap: Optional[Dict[str, str]] = None,
    ):
        """Connects to a MultiInput node."""
        conn = Connection(
            src=self.src,
            tgt=tgt,
            input_name=input_name,
            new_input_name=new_input_name,
            inputs_remap=inputs_remap,
        )
        self.builder.connections.append(conn)
        self.builder.log.info(
            "src=%s, tgt=%s, input=%s, new_input=%s, remap=%s",
            self.src,
            tgt,
            input_name,
            new_input_name,
            inputs_remap,
        )
        return conn


class FlowBuilder(Generic[CTX, T]):
    """Builds a flow from a list of connection definitions."""

    def __init__(self):
        self.connections: List[Connection] = []
        self.log = logging.getLogger(self.__class__.__name__)

    def connect(self, node: Node[CTX, T]) -> Connector[CTX, T]:
        """Connects a node."""
        return Connector(self, node)

    def build(self) -> Node[CTX, T]:
        """Evaluates the connection list and builds the flow."""
        last = self._find_last_node()
        return self._add_predecessors(last, self.connections.copy())

    def _find_last_node(self):
        sources = set(c.src for c in self.connections)
        targets = set(c.tgt for c in self.connections)
        terminals = targets - sources
        if len(terminals) != 1:
            raise ValueError(f"Wrong number of terminal nodes: {len(terminals)}")
        return terminals.pop()

    def _add_predecessors(self, flow: Node[CTX, T], conns: List[Connection]):
        self.log.debug("flow.before = %s", flow)
        self.log.debug("connections.before = %s", len(conns))

        def connect_to_join(
            src: Node[CTX, T],
            tgt: Join[CTX, T],
            name: str,
            new_name: Optional[str],
            remap: Optional[Dict[str, str]],
        ):
            if isinstance(src, Source):
                return src.to_join(tgt, name)
            if isinstance(src, Transformer):
                return src.to_join(tgt, name, new_name)
            if isinstance(src, Join):
                return src.to_join(tgt, name, remap)
            raise ValueError(f"Invalid node type as connection source: {src}")

        def connect_to_module(
            src: Node[CTX, T],
            tgt: Module[CTX, T],
            name: str,
            new_name: Optional[str],
            remap: Optional[Dict[str, str]],
        ):
            if isinstance(src, Source):
                return src.to_module(tgt, name)
            if isinstance(src, Transformer):
                return src.to_module(tgt, name, new_name)
            if isinstance(src, Join):
                return src.to_module(tgt, name, remap)
            raise ValueError(f"Invalid node type as connection source: {src}")

        def add_to_flow(conn: Connection):
            if isinstance(flow, Stub):
                return conn.src.to_stub(flow)  # type: ignore
            if isinstance(flow, Transformer):
                return conn.src.to_transformer(flow)  # type: ignore
            if isinstance(flow, Join):
                assert (
                    conn.input_name
                ), "Input name is mandatory when connecting to Join"
                return connect_to_join(
                    conn.src,
                    flow,
                    conn.input_name,
                    conn.new_input_name,
                    conn.inputs_remap,
                )
            if isinstance(flow, Module):
                assert (
                    conn.input_name
                ), "Input name is mandatory when connecting to Module"
                return connect_to_module(
                    conn.src,
                    flow,
                    conn.input_name,
                    conn.new_input_name,
                    conn.inputs_remap,
                )
            raise ValueError(f"Invalid flow type: {flow}")

        # find inbound connections for the current flow
        conns_to_flow = list(filter(lambda cn: cn.tgt == flow, conns))
        if len(conns_to_flow) == 0:
            return flow

        # add all inbound connections to the flow and remove them from available connection list
        for con in conns_to_flow:
            flow = add_to_flow(con)
            conns.remove(con)

        # adjust inbound connections that will be processed in the next iteration
        for inbound in conns_to_flow:
            node = inbound.src
            cons_to_node = [cn for cn in conns if cn.tgt == node]
            for con in cons_to_node:
                con.tgt = flow
                if isinstance(flow, MultiInput):
                    if isinstance(node, Transformer):
                        con.input_name = inbound.new_input_name or inbound.input_name
                    elif isinstance(node, Join):
                        assert con.input_name, "Missing input name"
                        remap = (
                            inbound.inputs_remap[con.input_name]
                            if inbound.inputs_remap
                            else None
                        )
                        con.input_name = remap or con.input_name

        self.log.debug("flow.after = %s", flow)
        self.log.debug("connections.after = %s", len(conns))
        return self._add_predecessors(flow, conns)

    def _combine_predecessors(self, last_added: Set[Node[CTX, T]], flow: Node[CTX, T]):
        def connect_to_stub(node: Node[CTX, T], stub: Stub[CTX, T]):
            return node.to_stub(stub)  # type: ignore

        def connect_to_transformer(node: Node[CTX, T], tx: Transformer[CTX, T]):
            return node.to_transformer(tx)  # type: ignore

        def adjust_transformer(
            tx: Transformer[CTX, T], name: str, new_name: Optional[str]
        ):
            for conn in self.connections:
                if conn.tgt == tx:
                    conn.set_input(new_name or name)

        def adjust_join(join: Join[CTX, T], remap: Optional[Dict[str, str]]):
            for conn in self.connections:
                if conn.tgt == join:
                    assert conn.input_name
                    if remap:
                        input_name = remap[conn.input_name] or conn.input_name
                    else:
                        input_name = conn.input_name
                    conn.set_input(input_name)

        def connect_to_join(
            node: Node[CTX, T],
            join: Join[CTX, T],
            name: str,
            new_name: Optional[str],
            remap: Optional[Dict[str, str]],
        ):
            if isinstance(node, Source):
                return node.to_join(join, name)
            if isinstance(node, Transformer):
                adjust_transformer(node, name, new_name)
                return node.to_join(join, name, new_name)
            if isinstance(node, Join):
                adjust_join(node, remap)
                return node.to_join(join, name, remap)
            raise ValueError(f"Invalid node type as connection source: {node}")

        def connect_to_module(
            node: Node[CTX, T],
            mod: Module[CTX, T],
            name: str,
            new_name: Optional[str],
            remap: Optional[Dict[str, str]],
        ):
            if isinstance(node, Source):
                return node.to_module(mod, name)
            if isinstance(node, Transformer):
                adjust_transformer(node, name, new_name)
                return node.to_module(mod, name, new_name)
            if isinstance(node, Join):
                adjust_join(node, remap)
                return node.to_module(mod, name, remap)
            raise ValueError(f"Invalid node type as connection source: {node}")

        def add_to_flow(conn: Connection, flw: Node[CTX, T]):
            if isinstance(flw, Stub):
                return connect_to_stub(conn.src, flw)
            if isinstance(flw, Transformer):
                return connect_to_transformer(conn.src, flw)
            if isinstance(flw, Join):
                assert conn.input_name
                return connect_to_join(
                    conn.src,
                    flw,
                    conn.input_name,
                    conn.new_input_name,
                    conn.inputs_remap,
                )
            if isinstance(flw, Module):
                assert conn.input_name
                return connect_to_module(
                    conn.src,
                    flw,
                    conn.input_name,
                    conn.new_input_name,
                    conn.inputs_remap,
                )
            raise ValueError(f"Invalid flow type: {flow}")

        self.log.info("before flow = %s", flow)

        newly_added: Set[Node[CTX, T]] = set()
        for target in last_added:
            conns = [c for c in self.connections if c.tgt == target]
            for con in conns:
                flow = add_to_flow(con, flow)
                newly_added.add(con.src)

        self.log.info("now flow = %s", flow)
        self.log.info("newly_added = %s", newly_added)

        if len(newly_added) == 0:
            return flow

        return self._combine_predecessors(newly_added, flow)
