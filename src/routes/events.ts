import { Hono } from "hono";
import { ApiKeysModel, EventsModel } from "@fileverse/api/base";
import type { Env } from "../types";

const events = new Hono<{ Bindings: Env }>();

async function getPortalAddress(apiKey: string): Promise<string | undefined> {
  const info = await ApiKeysModel.findByApiKey(apiKey);
  return info?.portalAddress;
}

// GET /api/events/failed
events.get("/failed", async (c) => {
  const portalAddress = await getPortalAddress(c.env.API_KEY);
  const failed = await EventsModel.listFailed(portalAddress);
  return c.json(failed);
});

// POST /api/events/retry-failed
events.post("/retry-failed", async (c) => {
  const portalAddress = await getPortalAddress(c.env.API_KEY);
  const retried = await EventsModel.resetAllFailedToPending(portalAddress);
  return c.json({ retried });
});

// POST /api/events/:id/retry
events.post("/:id/retry", async (c) => {
  const portalAddress = await getPortalAddress(c.env.API_KEY);
  const id = c.req.param("id");

  const ok = await EventsModel.resetFailedToPending(id, portalAddress);
  if (!ok) {
    return c.json({ message: "Event not found or not in failed state" }, 404);
  }
  return c.json({ ok: true });
});

export { events };
