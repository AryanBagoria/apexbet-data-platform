import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { Badge, EmptyState, ErrorState, KpiCard, StatusDot } from "../components/ui";

describe("ui primitives", () => {
  it("renders a badge with its label", () => {
    render(<Badge tone="ok">healthy</Badge>);
    expect(screen.getByText("healthy")).toBeInTheDocument();
  });

  it("renders a KPI card value and unit", () => {
    render(<KpiCard label="Events" value="1,234" unit="records" kind="total" />);
    expect(screen.getByText("Events")).toBeInTheDocument();
    expect(screen.getByText("1,234")).toBeInTheDocument();
    expect(screen.getByText("records")).toBeInTheDocument();
  });

  it("shows 'Unavailable' when a KPI is not available", () => {
    render(<KpiCard label="Latency" value="" available={false} />);
    expect(screen.getByText("Unavailable")).toBeInTheDocument();
  });

  it("labels metric kinds so totals are not confused with rates", () => {
    render(<KpiCard label="Throughput" value="2000" kind="rate" />);
    expect(screen.getByText("rate")).toBeInTheDocument();
  });

  it("renders error and empty states", () => {
    render(<ErrorState message="backend down" />);
    expect(screen.getByRole("alert")).toHaveTextContent("backend down");
    render(<EmptyState title="Nothing here" detail="run a pipeline" />);
    expect(screen.getByText("Nothing here")).toBeInTheDocument();
  });

  it("renders a status dot with an accessible-free visual marker", () => {
    const { container } = render(<StatusDot status="healthy" />);
    expect(container.querySelector(".dot")).toBeTruthy();
  });
});
