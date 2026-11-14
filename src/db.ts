// Postgres access of this Worker: a tagged template, `sql.query` and `sql.transaction`, on node-postgres over TCP.
// The same file is in ingress, runner and resource-broker (Workers share no code); change all three together.
//
// - `` sql`select ... ${value}` `` builds a query with `$1`, `$2`, ... parameters. It runs when awaited, so a list of
//   them can be handed to `sql.transaction` first, which runs them in order between BEGIN and COMMIT.
// - A Worker may not reuse a socket across requests, so each request makes its own `connect()` and ends it when the
//   request is done (`end()`, through `waitUntil`). With a Hyperdrive binding the connection goes to Hyperdrive, which
//   keeps warm connections to the database; without one it goes straight to `DATABASE_URL`.

import { Pool } from "pg";

/** A row as returned by Postgres (`timestamptz` as `Date`, `bigint` and `numeric` as strings, `json` parsed). */
export type Row = Record<string, any>;

/** A query that runs when awaited. */
export type Query = PromiseLike<Row[]> & { readonly text: string; readonly values: unknown[] };

export type Sql = {
  (strings: TemplateStringsArray, ...values: unknown[]): Query;
  /** Runs SQL text with `$1`, `$2`, ... parameters. */
  query(text: string, params?: unknown[]): Promise<Row[]>;
  /** Runs the queries in order in one transaction; the rows of each, in the same order. */
  transaction(queries: Query[]): Promise<Row[][]>;
  /** Closes the connections of this request. */
  end(): Promise<void>;
};

/** Where to connect: the Hyperdrive binding when there is one, otherwise `DATABASE_URL`. */
export function databaseUrl(env: { DATABASE_URL?: string; HYPERDRIVE?: { connectionString: string } }): string {
  const url = env.HYPERDRIVE?.connectionString ?? env.DATABASE_URL;
  if (!url) throw new Error("DATABASE_URL (or a HYPERDRIVE binding) is required");
  return url;
}

/** A database handle for one request. No connection is opened until the first query. */
export function connect(url: string): Sql {
  let pool: Pool | undefined;
  const open = () => {
    if (!pool) {
      pool = new Pool({ connectionString: url, max: 3 });
      pool.on("error", (error) => console.warn("[db] connection error", error.message));
    }
    return pool;
  };
  const run = async (text: string, values: unknown[]) => (await open().query(text, values)).rows as Row[];

  const sql = ((strings: TemplateStringsArray, ...values: unknown[]): Query => {
    const text = strings.reduce((built, part, index) => `${built}$${index}${part}`);
    let started: Promise<Row[]> | undefined;
    return {
      text,
      values,
      then(onFulfilled, onRejected) {
        started ??= run(text, values);
        return started.then(onFulfilled, onRejected);
      },
    };
  }) as Sql;

  sql.query = (text, params = []) => run(text, params);

  sql.transaction = async (queries) => {
    const client = await open().connect();
    try {
      await client.query("begin");
      const results: Row[][] = [];
      for (const query of queries) results.push((await client.query(query.text, query.values)).rows as Row[]);
      await client.query("commit");
      return results;
    } catch (error) {
      await client.query("rollback").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  };

  sql.end = async () => {
    const current = pool;
    pool = undefined;
    if (!current) return;
    // Closing the sockets reports "socket has been closed" as an error: expected here, not worth a warning.
    current.removeAllListeners("error");
    current.on("error", () => undefined);
    await current.end();
  };

  return sql;
}
