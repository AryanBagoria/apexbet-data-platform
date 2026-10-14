import { describe, expect, it } from "vitest";

import {
  formatBytes,
  formatMinor,
  formatNumber,
  formatTime,
  relativeTime,
  shortId,
} from "../services/format";

describe("format helpers", () => {
  it("formats numbers and treats null as unavailable", () => {
    expect(formatNumber(1234)).toBe("1,234");
    expect(formatNumber(null)).toBe("Unavailable");
    expect(formatNumber(undefined)).toBe("Unavailable");
  });

  it("compacts large numbers", () => {
    expect(formatNumber(12500, { compact: true })).toBe("12.5K");
  });

  it("formats bytes across units", () => {
    expect(formatBytes(0)).toBe("0 B");
    expect(formatBytes(1536)).toBe("1.5 KB");
    expect(formatBytes(null)).toBe("Unavailable");
  });

  it("formats minor units as currency", () => {
    expect(formatMinor(1250)).toBe("$12.50");
    expect(formatMinor(null)).toBe("Unavailable");
  });

  it("formats timestamps and relative time", () => {
    expect(formatTime(null)).toBe("Unavailable");
    expect(formatTime("2026-10-14T08:00:00Z")).not.toBe("Unavailable");
    const fiveMinAgo = new Date(Date.now() - 5 * 60_000).toISOString();
    expect(relativeTime(fiveMinAgo)).toMatch(/5m ago/);
  });

  it("shortens ids", () => {
    expect(shortId("abcdefghijklmnop")).toBe("abcdefghij…");
    expect(shortId(null)).toBe("—");
    expect(shortId("short")).toBe("short");
  });
});
