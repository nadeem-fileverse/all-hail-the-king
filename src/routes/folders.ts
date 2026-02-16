import { Hono } from "hono";
import { listFolders, getFolder, createFolder } from "@fileverse/api/base";
import type { Env } from "../types";

const folders = new Hono<{ Bindings: Env }>();

// GET /api/folders
folders.get("/", async (c) => {
  const limit = c.req.query("limit") ? parseInt(c.req.query("limit")!, 10) : undefined;
  const skip = c.req.query("skip") ? parseInt(c.req.query("skip")!, 10) : undefined;

  const result = await listFolders({ limit, skip });
  return c.json(result);
});

// POST /api/folders
folders.post("/", async (c) => {
  const body = await c.req.json();

  const required = [
    "onchainFileId",
    "folderId",
    "folderRef",
    "folderName",
    "portalAddress",
    "metadataIPFSHash",
    "lastTransactionBlockNumber",
    "lastTransactionBlockTimestamp",
  ];
  for (const field of required) {
    if (!body[field] && body[field] !== 0) {
      return c.json({ message: `Missing required field: ${field}` }, 400);
    }
  }

  const folder = await createFolder(body);
  return c.json(folder, 201);
});

// GET /api/folders/:folderRef/:folderId
folders.get("/:folderRef/:folderId", async (c) => {
  const folderRef = c.req.param("folderRef");
  const folderId = c.req.param("folderId");

  if (!folderRef || !folderId) {
    return c.json({ message: "folderRef and folderId are required" }, 400);
  }

  const folder = await getFolder(folderRef, folderId);
  if (!folder) {
    return c.json({ message: "Folder not found" }, 404);
  }

  return c.json(folder);
});

export { folders };
