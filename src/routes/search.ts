import { Hono } from "hono";
import { ApiKeysModel, searchNodes } from "@fileverse/api/base";
import type { Env } from "../types";

const search = new Hono<{ Bindings: Env }>();

// GET /api/search
search.get("/", async (c) => {
  const q = c.req.query("q");
  if (!q) {
    return c.json({ message: "Query parameter 'q' is required" }, 400);
  }

  const info = await ApiKeysModel.findByApiKey(c.env.API_KEY);
  const portalAddress = info?.portalAddress as string;
  if (!portalAddress) {
    return c.json({ message: "Invalid API key" }, 401);
  }

  const limit = c.req.query("limit") ? parseInt(c.req.query("limit")!, 10) : undefined;
  const skip = c.req.query("skip") ? parseInt(c.req.query("skip")!, 10) : undefined;

  const result = await searchNodes({ query: q, limit, skip, portalAddress });
  return c.json(result);
});

export { search };
