import assert from "node:assert/strict";
import { test } from "node:test";
import { RUN_NODE_WINDOW_MS, runNodeSlot } from "./notify";

test("run.node reports in one 5 s window share a slot and arrive at the window's end", () => {
  const start = 1_800_000_000_000; // on a window boundary
  assert.equal(start % RUN_NODE_WINDOW_MS, 0);
  const a = runNodeSlot(start + 100);
  const b = runNodeSlot(start + 4_900);
  assert.equal(a.slot, b.slot);
  assert.equal(a.delaySeconds, 5);
  assert.equal(b.delaySeconds, 1);
  // The next window gets its own message.
  assert.equal(runNodeSlot(start + RUN_NODE_WINDOW_MS).slot, a.slot + 1);
  // Never less than one second of delay.
  assert.equal(runNodeSlot(start + 4_999).delaySeconds, 1);
});
