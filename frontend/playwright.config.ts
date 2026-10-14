import { defineConfig, devices } from "@playwright/test";

/**
 * Playwright config for the dashboard end-to-end smoke tests.
 *
 * These tests exercise the real app: navigation, live API data on the overview,
 * the lineage graph, and the Pipeline Controls page driving a bounded backend
 * job. They assume the API (:8000) and the dev server (:5173) are already
 * running.
 *
 *   make serve        # terminal 1
 *   make frontend     # terminal 2
 *   make frontend-e2e # terminal 3
 *
 * The default base URL uses 127.0.0.1 rather than localhost: on machines where
 * localhost resolves to ::1 first, that can reach a different server listening
 * on the IPv6 loopback. Override with DASHBOARD_URL if needed.
 */
export default defineConfig({
  testDir: "./e2e",
  timeout: 60_000,
  expect: { timeout: 15_000 },
  fullyParallel: false,
  retries: 0,
  reporter: [["list"]],
  use: {
    baseURL: process.env.DASHBOARD_URL ?? "http://127.0.0.1:5173",
    trace: "retain-on-failure",
    viewport: { width: 1440, height: 900 },
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
});
