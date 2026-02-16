import {
  setAdapter,
  runMigrations,
  initializeFromApiKey,
} from "@fileverse/api/base";
import { D1Adapter } from "../adapter/d1-adapter";
import type { Env } from "../types";

let initialized = false;

export async function ensureInitialized(env: Env): Promise<void> {
  process.env.API_KEY = env.API_KEY;
  if (env.RPC_URL) {
    process.env.RPC_URL = env.RPC_URL;
  }
  process.env.NODE_ENV = env.NODE_ENV || "production";

  setAdapter(new D1Adapter(env.DB));

  if (initialized) return;

  console.log("[init] running migrations");
  await runMigrations();
  console.log("[init] migrations done");

  console.log("[init] initializing from API key");
  await initializeFromApiKey(env.API_KEY);
  console.log("[init] initialized successfully");

  initialized = true;
}
