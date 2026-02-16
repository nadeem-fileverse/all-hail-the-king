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
    try {
      await ensureInitialized(env);

      switch (event.cron) {
        case "*/1 * * * *":
          await submitPendingEvents();
          break;
        case "*/15 * * * *": // every 15 seconds
          await resolveSubmittedEvents();
          break;
      }
    } catch (error) {
      console.error(`[scheduled] cron ${event.cron} failed:`, error);
      throw error;
    }
  },
};
