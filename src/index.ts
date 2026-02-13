import { app } from "./routes";
import { ensureInitialized } from "./lib/init";
import { submitPendingEvents, resolveSubmittedEvents } from "./lib/sync";
import type { Env } from "./types";

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    await ensureInitialized(env);
    return app.fetch(request, env, ctx);
  },

  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
    await ensureInitialized(env);

    // Submit is quick — run inline so it completes before response
    await submitPendingEvents();

    // Resolve loop runs for ~50s (5 rounds × 10s sleep) — run via waitUntil
    // so the scheduled handler can return while resolve keeps polling
    ctx.waitUntil(resolveSubmittedEvents());
  },
};
