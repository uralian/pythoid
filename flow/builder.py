"""Module providing Builder interface to compose Pythoid dataflows."""
import logging
from dataclasses import dataclass
from typing import Dict, Generic, List, Optional

from .common import (
    CTX,
    Join,
    Module,
    MultiInput,
    Node,
    NoInput,
    Source,
    Stub,
    T,
    Task,
    Transformer,
)


@dataclass(kw_only=True, frozen=True)
class Connection(Generic[CTX, T]):
    """Represents a connection between two nodes."""

    origin: Node[CTX, T]
    target: Node[CTX, T]
    input_name: Optional[str] = None
    new_input_name: Optional[str] = None
    inputs_remap: Optional[Dict[str, str]] = None

    def set_origin(self, node: Node[CTX, T]) -> "Connection":
        """Sets the origin node."""
        object.__setattr__(self, "origin", node)
        return self

    def set_target(self, node: Node[CTX, T]) -> "Connection":
        """Sets the target node."""
        object.__setattr__(self, "target", node)
        return self

    def as_input(self, name: str) -> "Connection":
        """Sets the input name."""
        object.__setattr__(self, "input_name", name)
        return self

    def rename_input(self, new_name: str) -> "Connection":
        """Renames the input when the target is MultiInput and origin is Transformer."""
        object.__setattr__(self, "new_input_name", new_name)
        return self

    def remap_inputs(self, remap: Dict[str, str]) -> "Connection":
        """Re-labels origin's inputs when the target is MultiInput and origin is Join."""
        object.__setattr__(self, "inputs_remap", remap)
        return self

    def validate(self):
        """
        Checks if the connection is valid, i.e. represents a valid edge
        between two nodes of given types.
        """
        self._assert_origin_has_output()
        self._assert_target_has_inputs()
        self._assert_input_name()
        self._assert_input_rename()
        self._assert_input_remap()

    def _assert_origin_has_output(self):
        assert not isinstance(self.origin, Task), "Origin cannot be a Task"
        assert not isinstance(self.origin, Stub), "Origin cannot be a Stub"
        assert not isinstance(self.origin, Module), "Origin cannot be a Module"

    def _assert_target_has_inputs(self):
        assert not isinstance(self.target, NoInput), "Target node must have inputs"

    def _assert_input_name(self):
        if isinstance(self.target, MultiInput):
            assert self.input_name, "Input name must be specified for multi-input nodes"
            assert self.input_name in self.target.input_names()
        else:
            assert not self.input_name, "Input name not needed for single-input nodes"

    def _assert_input_rename(self):
        if self.new_input_name:
            assert isinstance(
                self.target, MultiInput
            ), "Cannot rename input: target is not a multi-input node"
            assert isinstance(
                self.origin, Transformer
            ), "Cannot rename input: origin is not a Transformer"

    def _assert_input_remap(self):
        if self.inputs_remap:
            assert isinstance(
                self.target, MultiInput
            ), "Cannot rename input: target is not a multi-input node"
            assert isinstance(
                self.origin, Join
            ), "Cannot remap inputs: origin is not a Join"

    def __repr__(self):
        return (
            f"Connection(org={self.origin}, tgt={self.target}, input={self.input_name}, "
            f"new_input={self.new_input_name}, remap={self.inputs_remap}"
        )


@dataclass(frozen=True)
class Connector(Generic[CTX, T]):
    """Helper class for providing fluent builder syntax."""

    builder: "FlowBuilder[CTX, T]"
    origin: Node[CTX, T]

    def to(self, tgt: Node[CTX, T]):
        """Connects to a SingleInput node."""
        conn = Connection(origin=self.origin, target=tgt)
        self.builder.connections.append(conn)
        self.builder.log.info("org=%s, tgt=%s", self.origin, tgt)
        return conn

    def to_input(self, tgt: MultiInput[CTX, T], input_name: str):
        """Connects to a MultiInput node."""
        conn = Connection(origin=self.origin, target=tgt, input_name=input_name)
        self.builder.connections.append(conn)
        self.builder.log.info("org=%s, tgt=%s, input=%s", self.origin, tgt, input_name)
        return conn


class FlowBuilder(Generic[CTX, T]):
    """Builds a flow from a list of connection definitions."""

    def __init__(self):
        self.connections: List[Connection] = []
        self.log = logging.getLogger(self.__class__.__name__)

    def connect(self, node: Node[CTX, T]) -> Connector[CTX, T]:
        """Returns a Connector object for the specified node."""
        return Connector(self, node)

    def build(self) -> Node[CTX, T]:
        """Evaluates the connection list and builds the flow."""
        for con in self.connections:
            con.validate()
        self.log.debug("connections=%s", self.connections)
        last = self._find_last_node()
        self.log.info("terminal=%s", last)
        return self._add_predecessors(last, self.connections.copy())

    def _find_last_node(self):
        origins = set(c.origin for c in self.connections)
        targets = set(c.target for c in self.connections)
        terminals = targets - origins
        if len(terminals) != 1:
            raise ValueError(f"Wrong number of terminal nodes: {len(terminals)}")
        return terminals.pop()

    def _add_predecessors(self, flow: Node[CTX, T], conns: List[Connection]):
        self.log.debug("flow.before = %s", flow)
        self.log.debug("connections.before = %s", len(conns))

        def connect_to_join(
            org: Node[CTX, T],
            tgt: Join[CTX, T],
            name: str,
            new_name: Optional[str],
            remap: Optional[Dict[str, str]],
        ):
            if isinstance(org, Source):
                return org.to_join(tgt, name)
            if isinstance(org, Transformer):
                return org.to_join(tgt, name, new_name)
            if isinstance(org, Join):
                return org.to_join(tgt, name, remap)
            raise ValueError(f"Invalid node type as connection origin: {org}")

        def connect_to_module(
            org: Node[CTX, T],
            tgt: Module[CTX, T],
            name: str,
            new_name: Optional[str],
            remap: Optional[Dict[str, str]],
        ):
            if isinstance(org, Source):
                return org.to_module(tgt, name)
            if isinstance(org, Transformer):
                return org.to_module(tgt, name, new_name)
            if isinstance(org, Join):
                return org.to_module(tgt, name, remap)
            raise ValueError(f"Invalid node type as connection origin: {org}")

        def add_to_flow(conn: Connection):
            if isinstance(flow, Stub):
                return conn.origin.to_stub(flow)  # type: ignore
            if isinstance(flow, Transformer):
                return conn.origin.to_transformer(flow)  # type: ignore
            if isinstance(flow, Join):
                assert conn.input_name, "Missing input name when connecting to Join"
                return connect_to_join(
                    conn.origin,
                    flow,
                    conn.input_name,
                    conn.new_input_name,
                    conn.inputs_remap,
                )
            if isinstance(flow, Module):
                assert conn.input_name, "Missing input name when connecting to Module"
                return connect_to_module(
                    conn.origin,
                    flow,
                    conn.input_name,
                    conn.new_input_name,
                    conn.inputs_remap,
                )
            raise ValueError(f"Invalid flow type: {flow}")

        # find inbound connections for the current flow
        conns_to_flow = list(filter(lambda cn: cn.target == flow, conns))
        if len(conns_to_flow) == 0:
            return flow

        # add all inbound connections to the flow and remove them from available connection list
        for con in conns_to_flow:
            flow = add_to_flow(con)
            conns.remove(con)

        # adjust inbound connections that will be processed in the next iteration
        for inbound in conns_to_flow:
            node = inbound.origin
            cons_to_node = [cn for cn in conns if cn.target == node]
            for con in cons_to_node:
                con.set_target(flow)
                if isinstance(flow, MultiInput):
                    if isinstance(node, Transformer):
                        assert inbound.input_name, "Missing input name for multi-input"
                        con.as_input(inbound.new_input_name or inbound.input_name)
                    elif isinstance(node, Join):
                        assert con.input_name, "Missing input name for multi-input"
                        remapped = (
                            inbound.inputs_remap[con.input_name]
                            if inbound.inputs_remap
                            else None
                        )
                        con.as_input(remapped or con.input_name)

        self.log.debug("flow.after = %s", flow)
        self.log.debug("connections.after = %s", len(conns))
        return self._add_predecessors(flow, conns)
