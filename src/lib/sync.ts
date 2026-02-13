import { EventsModel, submitEvent, resolveEvent } from "@fileverse/api/cloudflare";

const MAX_SUBMIT_PER_TICK = 1;
const MAX_RESOLVE_PER_ROUND = 3;
const RESOLVE_ROUNDS = 5;
const RESOLVE_INTERVAL_MS = 10_000;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Submit phase — picks up pending events, does crypto + signing + sends tx.
 * Runs once per cron tick (~1 min).
 */
export async function submitPendingEvents(): Promise<void> {
  console.log("[sync:submit] starting");

  for (let i = 0; i < MAX_SUBMIT_PER_TICK; i++) {
    const event = await EventsModel.findNextEligible([]);
    if (!event) break;

    await EventsModel.markProcessing(event._id);

    try {
      console.log(`[sync:submit] event ${event._id}, type: ${event.type}, fileId: ${event.fileId}`);
      await submitEvent(event);
      await EventsModel.markSubmitted(event._id);
      console.log(`[sync:submit] event ${event._id} submitted successfully`);
    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : String(error);
      console.error(`[sync:submit] event ${event._id} threw:`, errorMsg);
      await EventsModel.markFailed(event._id, errorMsg);
    }
  }

  console.log("[sync:submit] done");
}

/**
 * Resolve phase — polls submitted events for receipts.
 * Runs 5 rounds × 10s apart (~50s) to check every ~10 seconds.
 * Sleep time is wall-clock only, doesn't consume CPU.
 */
export async function resolveSubmittedEvents(): Promise<void> {
  console.log("[sync:resolve] starting");

  for (let round = 0; round < RESOLVE_ROUNDS; round++) {
    const checkedFileIds: string[] = [];

    for (let i = 0; i < MAX_RESOLVE_PER_ROUND; i++) {
      const event = await EventsModel.findNextSubmitted(checkedFileIds);
      if (!event) break;

      checkedFileIds.push(event.fileId);

      try {
        console.log(`[sync:resolve] round ${round}, event ${event._id}, type: ${event.type}`);
        const result = await resolveEvent(event);

        if (result.resolved) {
          await EventsModel.markProcessed(event._id);
          console.log(`[sync:resolve] event ${event._id} resolved`);
        }
        // If not resolved, leave as submitted — will retry next round / next tick
      } catch (error) {
        const errorMsg = error instanceof Error ? error.message : String(error);
        console.error(`[sync:resolve] event ${event._id} threw:`, errorMsg);
        await EventsModel.markFailed(event._id, errorMsg);
      }
    }

    if (round < RESOLVE_ROUNDS - 1) {
      await sleep(RESOLVE_INTERVAL_MS);
    }
  }

  console.log("[sync:resolve] done");
}
