import { Hono } from "hono";
import { authMiddleware } from "../middleware/auth";
import { errorHandler } from "../middleware/error";
import { ddocs } from "./ddocs";
import { folders } from "./folders";
import { search } from "./search";
import { events } from "./events";
import { mcp } from "./mcp";
import type { Env } from "../types";

const app = new Hono<{ Bindings: Env }>();

// Global error handler
app.use("*", errorHandler);

// Health check
app.get("/ping", (c) => c.json({ reply: "pong" }));

// Auth middleware for all /api/* routes
app.use("/api/*", authMiddleware);

// Mount route groups
app.route("/api/ddocs", ddocs);
app.route("/api/folders", folders);
app.route("/api/search", search);
app.route("/api/events", events);

// MCP endpoint (no auth required)
app.route("/mcp", mcp);

export { app };
