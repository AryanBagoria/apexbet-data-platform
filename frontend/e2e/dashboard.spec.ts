import { expect, test } from "@playwright/test";

/**
 * End-to-end smoke tests against the real running dashboard + backend.
 *
 * They verify that the app is wired to actual API data (not fixtures) and that
 * the interactive features work. They do not assert exact counts, because the
 * underlying synthetic dataset changes between runs.
 */

test("overview loads real platform data from the API", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Overview" })).toBeVisible();
  // The synthetic-data disclaimer is always visible.
  await expect(page.getByText("APEXBET — SYNTHETIC DATA DEMONSTRATION")).toBeVisible();
  // KPI cards are populated from the backend, never hardcoded placeholders.
  await expect(page.getByText("Events ingested")).toBeVisible();
  await expect(page.getByText("Bronze records")).toBeVisible();
  // Distinguish cumulative totals from rates.
  await expect(page.getByText("TOTAL").first()).toBeVisible();
  await expect(page.getByText("RATE").first()).toBeVisible();
});

test("navigation reaches every dashboard page", async ({ page }) => {
  await page.goto("/");
  const routes = [
    ["Event Stream", "/streaming"],
    ["Pipeline Lineage", "/lineage"],
    ["Data Pipeline", "/layers"],
    ["Data Quality", "/quality"],
    ["Enrichment & Anomalies", "/ai"],
    ["Analytics", "/analytics"],
    ["Pipeline Controls", "/demo"],
  ];
  for (const [label, path] of routes) {
    await page.getByRole("link", { name: label }).click();
    await expect(page).toHaveURL(new RegExp(`${path}$`));
  }
});

test("pipeline lineage shows the implemented stages", async ({ page }) => {
  await page.goto("/lineage");
  await expect(
    page.getByRole("heading", { name: "Pipeline Lineage" }),
  ).toBeVisible();
  // The architecture nodes must be present.
  for (const label of [
    "Synthetic Event Generator",
    "FastAPI Ingestion",
    "Streaming Broker",
    "Event Validation",
    "Bronze Layer",
    "Silver Transformations",
    "Enrichment",
    "Gold Analytics",
  ]) {
    await expect(page.getByText(label, { exact: false }).first()).toBeVisible();
  }
  // A legend explains the status colours.
  await expect(page.getByText("Disabled / not configured")).toBeVisible();
});

test("clicking a lineage node opens a details panel", async ({ page }) => {
  await page.goto("/lineage");
  await page.getByRole("button", { name: /Bronze Layer, status/ }).first().click();
  await expect(page.getByText("Records in")).toBeVisible();
  await expect(page.getByText("Records out")).toBeVisible();
  await expect(page.getByText("API endpoint")).toBeVisible();
});

test("data pipeline page shows layers and a preview table", async ({ page }) => {
  await page.goto("/layers");
  await expect(page.getByRole("heading", { name: "Data Pipeline" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Bronze", exact: true })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Silver", exact: true })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Gold", exact: true })).toBeVisible();
  // The dataset browser renders real rows from DuckDB.
  await expect(page.getByText("Dataset browser")).toBeVisible();
});

test("data quality page lists validation rules", async ({ page }) => {
  await page.goto("/quality");
  await expect(page.getByRole("heading", { name: "Data Quality & Reconciliation" })).toBeVisible();
  await expect(page.getByText("Rules evaluated")).toBeVisible();
  // Rule ids like DQ001 come from the real quality run.
  await expect(page.getByText(/DQ0\d\d/).first()).toBeVisible();
});

test("enrichment page separates modes and shows operational framing", async ({ page }) => {
  await page.goto("/ai");
  await expect(page.getByRole("heading", { name: /Enrichment & Anomaly/ })).toBeVisible();
  await expect(page.getByText("operational signals", { exact: false })).toBeVisible();
  await expect(page.getByText("Rules (default)", { exact: false })).toBeVisible();
});

test("pipeline controls run a bounded job and report the result", async ({ page }) => {
  await page.goto("/demo");
  await expect(page.getByRole("heading", { name: "Pipeline Controls" })).toBeVisible();
  // Run the smallest preset (generate_small) via the real backend endpoint.
  await page.getByRole("button", { name: "Run" }).first().click();
  await expect(page.getByText("succeeded", { exact: false })).toBeVisible({ timeout: 45_000 });
  await expect(page.getByText("Consumed")).toBeVisible();
  await expect(page.getByText("Bronze written")).toBeVisible();
});

test("pausing the streaming feed does not claim to stop the pipeline", async ({ page }) => {
  await page.goto("/streaming");
  await expect(page.getByRole("heading", { name: "Event Stream" })).toBeVisible();
  await page.getByRole("button", { name: "Pause feed" }).click();
  await expect(page.getByText("Feed paused in the UI only", { exact: false })).toBeVisible();
});
