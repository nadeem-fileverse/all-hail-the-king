import { Hono } from "hono";
import {
  ApiKeysModel,
  listFiles,
  getFile,
  createFile,
  updateFile,
  deleteFile,
} from "@fileverse/api/cloudflare";
import type { Env } from "../types";

const ddocs = new Hono<{ Bindings: Env }>();

async function getPortalAddress(apiKey: string): Promise<string> {
  const info = await ApiKeysModel.findByApiKey(apiKey);
  if (!info?.portalAddress) {
    throw Object.assign(new Error("Invalid API key"), { status: 401 });
  }
  return info.portalAddress;
}

// POST /api/ddocs
ddocs.post("/", async (c) => {
  const portalAddress = await getPortalAddress(c.env.API_KEY);
  const body = await c.req.json();

  const file = await createFile({
    title: body.title,
    content: body.content,
    portalAddress,
  });

  return c.json(
    { message: "File created successfully. Sync to on-chain is pending.", data: file },
    201,
  );
});

// GET /api/ddocs
ddocs.get("/", async (c) => {
  const portalAddress = await getPortalAddress(c.env.API_KEY);
  const limit = c.req.query("limit") ? parseInt(c.req.query("limit")!, 10) : undefined;
  const skip = c.req.query("skip") ? parseInt(c.req.query("skip")!, 10) : undefined;

  const result = await listFiles({ limit, skip, portalAddress });
  return c.json(result);
});

// GET /api/ddocs/:ddocId
ddocs.get("/:ddocId", async (c) => {
  const portalAddress = await getPortalAddress(c.env.API_KEY);
  const ddocId = c.req.param("ddocId");

  const file = await getFile(ddocId, portalAddress);
  if (!file) {
    return c.json({ message: "File not found" }, 404);
  }

  return c.json(file);
});

// PUT /api/ddocs/:ddocId
ddocs.put("/:ddocId", async (c) => {
  const portalAddress = await getPortalAddress(c.env.API_KEY);
  const ddocId = c.req.param("ddocId");
  const body = await c.req.json();

  if (!body.title && !body.content) {
    return c.json(
      { message: "At least one field is required: Either provide title, content, or both" },
      400,
    );
  }

  const updated = await updateFile(ddocId, body, portalAddress);
  return c.json({ message: "File updated successfully", data: updated });
});

// DELETE /api/ddocs/:ddocId
ddocs.delete("/:ddocId", async (c) => {
  const portalAddress = await getPortalAddress(c.env.API_KEY);
  const ddocId = c.req.param("ddocId");

  const deleted = await deleteFile(ddocId, portalAddress);
  return c.json({ message: "File deleted successfully", data: deleted });
});

export { ddocs };
