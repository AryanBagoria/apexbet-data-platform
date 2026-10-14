/**
 * Deterministic dashboard screenshot capture.
 *
 * Runs a real headless Chromium against the locally running dashboard
 * (Vite dev server proxying to the FastAPI backend), visits each page, waits
 * for the page heading, and writes a PNG into ../../docs/assets.
 *
 * Usage (with the API on :8000 and `npm run dev` on :5173):
 *   npm run screenshots
 *
 * These are real screenshots of the running application: nothing is drawn or
 * faked, and charts show whatever the current synthetic pipeline produced.
 */
import { chromium } from "@playwright/test";
import { mkdir } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

const here = dirname(fileURLToPath(import.meta.url));
const OUT = resolve(here, "../../docs/assets");
const BASE = process.env.DASHBOARD_URL ?? "http://127.0.0.1:5173";

const PAGES = [
  { path: "/", file: "01-overview.png", wait: "Overview" },
  { path: "/streaming", file: "02-event-stream.png", wait: "Event Stream" },
  { path: "/lineage", file: "03-pipeline-lineage.png", wait: "Pipeline Lineage" },
  { path: "/layers", file: "04-data-pipeline.png", wait: "Data Pipeline" },
  { path: "/quality", file: "05-data-quality.png", wait: "Data Quality & Reconciliation" },
  { path: "/ai", file: "06-enrichment-anomalies.png", wait: "Enrichment & Anomaly Detection" },
  { path: "/analytics", file: "07-analytics.png", wait: "Analytics" },
  { path: "/demo", file: "08-pipeline-controls.png", wait: "Pipeline Controls" },
];

async function main() {
  await mkdir(OUT, { recursive: true });
  const browser = await chromium.launch();
  const page = await browser.newPage({
    viewport: { width: 1440, height: 900 },
    deviceScaleFactor: 2,
  });

  for (const p of PAGES) {
    await page.goto(`${BASE}${p.path}`, { waitUntil: "networkidle" });
    await page.getByText(p.wait, { exact: false }).first().waitFor({ timeout: 15000 });
    // let charts / react-flow finish their first paint
    await page.waitForTimeout(800);
    await page.screenshot({ path: resolve(OUT, p.file), fullPage: true });
    console.log(`✔ captured ${p.file}`);
  }

  await browser.close();
  console.log(`\nScreenshots written to ${OUT}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
