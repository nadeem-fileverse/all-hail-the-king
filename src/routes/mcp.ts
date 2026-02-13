import { Hono } from "hono";
import { createMcpServer } from "@fileverse/api/cloudflare";
import type { FileverseConfig } from "@fileverse/api/cloudflare";
import type { Env } from "../types";

const mcp = new Hono<{ Bindings: Env }>();

// MCP endpoint — creates a stateless MCP server per request.
// NOTE: StreamableHTTPServerTransport expects Node.js req/res objects.
// If this doesn't work under nodejs_compat, MCP will be deferred to v2.
// For now, we expose an informational endpoint.
mcp.all("/", async (c) => {
  // TODO: Integrate StreamableHTTPServerTransport with Hono once
  // compatibility with Cloudflare Workers is confirmed.
  return c.json({
    message: "MCP endpoint. Use the Fileverse MCP CLI tool for full MCP support.",
    hint: "npx @fileverse/api mcp",
  });
});

export { mcp };
