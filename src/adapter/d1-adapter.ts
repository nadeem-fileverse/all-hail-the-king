import type { DatabaseAdapter, ExecuteResult } from "@fileverse/api/base";

export class D1Adapter implements DatabaseAdapter {
  private db: D1Database;
  private connected = true;

  readonly dialect = "sqlite" as const;

  constructor(db: D1Database) {
    this.db = db;
  }

  async select<T>(sql: string, params: any[] = []): Promise<T[]> {
    const result = await this.db.prepare(sql).bind(...params).all<T>();
    return result.results;
  }

  async selectOne<T>(sql: string, params: any[] = []): Promise<T | undefined> {
    const result = await this.db.prepare(sql).bind(...params).first<T>();
    return result ?? undefined;
  }

  async execute(sql: string, params: any[] = []): Promise<ExecuteResult> {
    const result = await this.db.prepare(sql).bind(...params).run();
    return {
      changes: result.meta.changes,
      lastInsertRowid: result.meta.last_row_id,
    };
  }

  async transaction<T>(callback: () => Promise<T>): Promise<T> {
    // D1 doesn't support interactive transactions.
    // Execute the callback directly — acceptable because current usage
    // wraps simple single-statement operations.
    return callback();
  }

  async exec(sql: string): Promise<void> {
    const statements = sql
      .split(";")
      .map((s) => s.replace(/--.*$/gm, "").trim())
      .filter(Boolean);
    for (const stmt of statements) {
      try {
        await this.db.prepare(stmt).run();
      } catch (err: unknown) {
        const msg = err instanceof Error ? err.message : "";
        if (!msg.includes("duration")) throw err;
      }
    }
  }

  async close(): Promise<void> {
    this.connected = false;
  }

  isConnected(): boolean {
    return this.connected;
  }
}
