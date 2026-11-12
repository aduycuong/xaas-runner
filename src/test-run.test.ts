import assert from "node:assert/strict";
import { test } from "node:test";
import { graphOfRun } from "./engine";

const action = (id: string) => ({ id, type: "action", position: { x: 0, y: 0 }, data: { nodeKey: "core.echo", version: "1.0.0", inputs: {}, settings: {} } });
const draft = {
  nodes: [{ id: "t", type: "trigger", position: { x: 0, y: 0 }, data: {} }, action("a"), action("b"), action("loose")],
  edges: [
    { source: "t", target: "a" },
    { source: "a", target: "b" },
  ],
};

test("a whole-draft test runs the stored graph and refuses one with issues", () => {
  assert.equal(graphOfRun({ ...draft, nodes: draft.nodes.slice(0, 3), edges: draft.edges }, {}).graph?.nodes.length, 3);
  assert.equal(graphOfRun({ nodes: [{ id: "t", type: "trigger", position: { x: 0, y: 0 }, data: {} }, action("x")], edges: [] }, {}).graph, null);
});

test("a one-node test keeps only the trigger and that node, even when the rest of the draft is unfinished", () => {
  const { graph } = graphOfRun(draft, { nodeId: "b", outputs: { a: { x: 1 } } });
  assert.deepEqual(graph?.nodes.map((node) => node.id), ["t", "b"]);
  assert.deepEqual(graph?.edges.map((edge) => [edge.source, edge.target]), [["t", "b"]]);
  assert.equal(graphOfRun(draft, { nodeId: "missing" }).graph, null);
  assert.equal(graphOfRun(draft, { nodeId: "t" }).graph, null);
});
