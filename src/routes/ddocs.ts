import { Hono } from "hono";
import type { HonoRequest } from "hono";
import {
  ApiKeysModel,
  listFiles,
  getFile,
  createFile,
  updateFile,
  deleteFile,
} from "@fileverse/api/base";
import type { Env } from "../types";

const ddocs = new Hono<{ Bindings: Env }>();

async function getPortalAddress(apiKey: string): Promise<string> {
  const info = await ApiKeysModel.findByApiKey(apiKey);
  if (!info?.portalAddress) {
    throw Object.assign(new Error("Invalid API key"), { status: 401 });
  }
  return info.portalAddress;
}

async function parseBodyFields(
  req: HonoRequest,
): Promise<{ title?: string; content?: string } | null> {
  const contentType = req.header("content-type") || "";

  if (contentType.includes("application/json")) {
    const body = await req.json();
    return { title: body.title, content: body.content };
  }

  if (
    contentType.includes("multipart/form-data") ||
    contentType.includes("application/x-www-form-urlencoded")
  ) {
    const form = await req.parseBody();
    let title = typeof form.title === "string" ? form.title : 'Untitled';
    let content: string | undefined;
    const file = form.file;
    if (file instanceof File) {
      content = await file.text();
      if (!title) title = file.name;
    } else if (typeof form.content === "string") {
      content = form.content;
    }
    return { title, content };
  }

  return null;
}

ddocs.post("/", async (c) => {
  const portalAddress = await getPortalAddress(c.env.API_KEY);
  const fields = await parseBodyFields(c.req);

  if (!fields) return c.json({ message: "Unsupported content type" }, 415);
  if (!fields.title || !fields.content) {
    return c.json({ message: "Both title and content are required" }, 400);
  }

  const file = await createFile({
    title: fields.title,
    content: fields.content,
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

ddocs.put("/:ddocId", async (c) => {
  const portalAddress = await getPortalAddress(c.env.API_KEY);
  const ddocId = c.req.param("ddocId");
  const fields = await parseBodyFields(c.req);

  if (!fields) return c.json({ message: "Unsupported content type" }, 415);
  if (!fields.title && !fields.content) {
    return c.json(
      { message: "At least one field is required: Either provide title, content, or both" },
      400,
    );
  }

  const updated = await updateFile(ddocId, fields, portalAddress);
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
