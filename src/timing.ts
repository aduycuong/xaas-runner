// Where the time of one runner request goes. Upstash calls the runner once per step; this logs one JSON
// line per request (`wrangler tail` or Workers Logs), so the gap between two steps can be split into:
//   toFunction  request arrived → the workflow function starts (SDK: signature, body, history)
//   steps       each step that ran in this request (replayed steps do not run), with its database and node time
//   afterSteps  last step ended → response sent (SDK: hands the step result to QStash)
// In a Worker the clock only moves across I/O, which is what is measured here.

import type { Query, Sql } from "./db";
import type { CallNode } from "./engine";

type StepTiming = { name: string; ms: number; dbQueries: number; dbMs: number; nodeMs: number };

export type Timing = {
  /** The workflow function started (called on each replay; only the first call counts). */
  functionStarted(): void;
  /** Wraps the body of one step. */
  step<T>(name: string, fn: () => Promise<T>): Promise<T>;
  sql(sql: Sql): Sql;
  callNode(callNode: CallNode): CallNode;
  /** Logs the line. */
  done(extra: Record<string, unknown>): void;
};

export function createTiming(): Timing {
  const arrived = Date.now();
  let fnStarted: number | null = null;
  let lastStepEnded: number | null = null;
  const steps: StepTiming[] = [];
  let current: StepTiming | null = null;
  let dbQueries = 0;
  let dbMs = 0;

  const timeDb = async <T>(run: () => PromiseLike<T>): Promise<T> => {
    const started = Date.now();
    try {
      return await run();
    } finally {
      const ms = Date.now() - started;
      dbQueries += 1;
      dbMs += ms;
      if (current) {
        current.dbQueries += 1;
        current.dbMs += ms;
      }
    }
  };

  return {
    functionStarted() {
      fnStarted ??= Date.now();
    },

    async step(name, fn) {
      const entry: StepTiming = { name, ms: 0, dbQueries: 0, dbMs: 0, nodeMs: 0 };
      const started = Date.now();
      current = entry;
      try {
        return await fn();
      } finally {
        entry.ms = Date.now() - started;
        lastStepEnded = Date.now();
        current = null;
        steps.push(entry);
      }
    },

    sql(sql) {
      const wrapped = ((strings: TemplateStringsArray, ...values: unknown[]): Query => {
        const query = sql(strings, ...values);
        let timed: Promise<unknown> | undefined;
        return {
          text: query.text,
          values: query.values,
          then(onFulfilled, onRejected) {
            timed ??= timeDb(() => query);
            return (timed as Promise<never>).then(onFulfilled, onRejected);
          },
        };
      }) as Sql;
      // A query handed to `transaction` is run by it, not awaited: the transaction counts once.
      wrapped.query = (text, params) => timeDb(() => sql.query(text, params));
      wrapped.transaction = (queries) => timeDb(() => sql.transaction(queries));
      wrapped.end = () => sql.end();
      return wrapped;
    },

    callNode(callNode) {
      return async (call) => {
        const started = Date.now();
        try {
          return await callNode(call);
        } finally {
          if (current) current.nodeMs += Date.now() - started;
        }
      };
    },

    done(extra) {
      const ended = Date.now();
      console.log(
        JSON.stringify({
          timing: "runner-request",
          ...extra,
          totalMs: ended - arrived,
          toFunctionMs: fnStarted === null ? null : fnStarted - arrived,
          steps,
          dbQueries,
          dbMs,
          afterStepsMs: lastStepEnded === null ? null : ended - lastStepEnded,
        }),
      );
    },
  };
}
