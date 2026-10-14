import { afterEach, describe, expect, it, vi } from "vitest";

import { api } from "../services/api";

// The API client must talk to the real backend contract; we mock fetch to
// assert URL construction, error mapping and timeouts without a live server.

function mockFetch(handler: (url: string, init?: RequestInit) => Response) {
  return vi.fn((input: RequestInfo | URL, init?: RequestInit) =>
    Promise.resolve(handler(String(input), init)),
  );
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("api client", () => {
  it("requests the overview endpoint and returns typed data", async () => {
    const payload = { available: true, detail: null, run_id: "run_x", finished_at: null, kpis: [] };
    vi.stubGlobal(
      "fetch",
      mockFetch(() =>
        new Response(JSON.stringify(payload), { status: 200 }),
      ),
    );
    const out = await api.overview();
    expect(out.available).toBe(true);
    expect(out.run_id).toBe("run_x");
  });

  it("encodes dataset names in the preview URL", async () => {
    const calls: string[] = [];
    vi.stubGlobal(
      "fetch",
      mockFetch((url) => {
        calls.push(url);
        return new Response(JSON.stringify({ dataset: "x", columns: [], rows: [], total: 0, offset: 0, limit: 25, available: true, detail: null }), { status: 200 });
      }),
    );
    await api.preview("gold_event_counts", 10, 5);
    expect(calls[0]).toContain("dataset=gold_event_counts");
    expect(calls[0]).toContain("offset=10");
    expect(calls[0]).toContain("limit=5");
  });

  it("maps HTTP errors to a helpful message", async () => {
    vi.stubGlobal(
      "fetch",
      mockFetch(() =>
        new Response(JSON.stringify({ detail: "boom" }), { status: 500 }),
      ),
    );
    await expect(api.quality()).rejects.toThrow(/boom/);
  });

  it("reports a friendly error when the backend is unreachable", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(() => Promise.reject(new TypeError("network down"))),
    );
    await expect(api.health()).rejects.toThrow(/Cannot reach the backend/);
  });

  it("posts bounded demo requests", async () => {
    let body: string | undefined;
    vi.stubGlobal(
      "fetch",
      mockFetch((_url, init) => {
        body = init?.body as string;
        return new Response(JSON.stringify({ job_id: "j1", kind: "traffic_spike", status: "queued" }), { status: 200 });
      }),
    );
    const job = await api.startDemo("traffic_spike", 2500, 42);
    expect(job.job_id).toBe("j1");
    expect(body).toContain("2500");
    expect(body).toContain("42");
  });

  it("fetches the enrichment summary totals", async () => {
    const calls: string[] = [];
    vi.stubGlobal(
      "fetch",
      mockFetch((url) => {
        calls.push(url);
        return new Response(
          JSON.stringify({
            available: true,
            detail: null,
            total: 2000,
            succeeded: 2000,
            failed: 0,
            skipped: 0,
            methods: { normalized_description: 2000 },
            models: { "rules-enricher": 2000 },
          }),
          { status: 200 },
        );
      }),
    );
    const s = await api.enrichmentSummary();
    expect(calls[0]).toContain("/enrichment/summary");
    expect(s.total).toBe(2000);
    expect(s.succeeded).toBe(2000);
  });
});
