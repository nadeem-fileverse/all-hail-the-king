import { EventsModel, FilesModel, submitEvent, resolveEvent } from "@fileverse/api/cloudflare";

const MAX_SUBMIT_PER_TICK = 1;
const MAX_RESOLVE_PER_TICK = 3;

export async function submitPendingEvents(): Promise<void> {
  console.log("[sync:submit] starting");

  for (let i = 0; i < MAX_SUBMIT_PER_TICK; i++) {
    const event = await EventsModel.findNextEligible([]);
    if (!event) break;

    try {
      await EventsModel.markProcessing(event._id);
      console.log(`[sync:submit] event ${event._id}, type: ${event.type}, fileId: ${event.fileId}`);
      const result = await submitEvent(event);
      if (result && "userOpHash" in result) {
        await EventsModel.setEventPendingOp(event._id, result.userOpHash, result.pendingPayload);
      } else if (result && "noOp" in result) {
        await FilesModel.update(result.fileId, { syncStatus: "synced", isDeleted: 1 }, result.portalAddress);
      }
      await EventsModel.markSubmitted(event._id);
      console.log(`[sync:submit] event ${event._id} submitted successfully`);
    } catch (error) {
      console.error(`[sync:submit] event ${event._id} failed:`, error);
      const errorMsg = error instanceof Error ? error.message : String(error);
      await EventsModel.markFailed(event._id, errorMsg);
    }
  }

  console.log("[sync:submit] done");
}

export async function resolveSubmittedEvents(): Promise<void> {
  console.log("[sync:resolve] starting");

  const checkedFileIds: string[] = [];

  for (let i = 0; i < MAX_RESOLVE_PER_TICK; i++) {
    const event = await EventsModel.findNextSubmitted(checkedFileIds);
    if (!event) break;

    checkedFileIds.push(event.fileId);

    try {
      console.log(`[sync:resolve] event ${event._id}, type: ${event.type}`);
      const result = await resolveEvent(event);

      if (result.resolved) {
        await EventsModel.markProcessed(event._id);
        console.log(`[sync:resolve] event ${event._id} resolved`);
      }
    } catch (error) {
      console.error(`[sync:resolve] event ${event._id} failed:`, error);
      const errorMsg = error instanceof Error ? error.message : String(error);
      await EventsModel.markFailed(event._id, errorMsg);
    }
  }

  console.log("[sync:resolve] done");
}
