import { EventsModel, submitEvent, resolveEvent } from "@fileverse/api/base";

const MAX_SUBMIT_PER_TICK = 2;
const MAX_RESOLVE_PER_TICK = 3;

export async function submitPendingEvents(): Promise<void> {
  for (let i = 0; i < MAX_SUBMIT_PER_TICK; i++) {
    const event = await EventsModel.findNextEligible([]);
    if (!event) break;
    try {
      console.log(`[sync:submit] event ${event._id}, type: ${event.type}`);
      await submitEvent(event);
      console.log(`[sync:submit] event ${event._id} submitted successfully`);
    } catch (error) {
      console.error(`[sync:submit] event ${event._id} failed:`, error);
    }
  }
}

export async function resolveSubmittedEvents(): Promise<void> {

  for (let i = 0; i < MAX_RESOLVE_PER_TICK; i++) {
    const event = await EventsModel.findNextSubmitted([]);
    if (!event) break;
    try {
      console.log(`[sync:resolve] event ${event._id}, type: ${event.type}`);
      await resolveEvent(event);
      console.log(`[sync:resolve] event ${event._id} resolved`);
    } catch (error) {
      console.error(`[sync:resolve] event ${event._id} failed:`, error);
    }
  }
}
