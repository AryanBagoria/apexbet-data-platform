import { useQuery } from "@tanstack/react-query";
import { api } from "../services/api";

// Typed React Query hooks over the dashboard API. Each hook maps 1:1 to a real
// backend endpoint; components never fetch ad hoc.

export function useHealth() {
  return useQuery({
    queryKey: ["health"],
    queryFn: api.health,
    refetchInterval: 10_000,
  });
}

export function useOverview() {
  return useQuery({
    queryKey: ["overview"],
    queryFn: api.overview,
    refetchInterval: 8_000,
  });
}

export function useThroughput(minutes = 180) {
  return useQuery({
    queryKey: ["throughput", minutes],
    queryFn: () => api.throughput(minutes),
    refetchInterval: 8_000,
  });
}

export function useLatency() {
  return useQuery({ queryKey: ["latency"], queryFn: api.latency });
}

export function useLayers() {
  return useQuery({
    queryKey: ["layers"],
    queryFn: api.layers,
    refetchInterval: 10_000,
  });
}

export function useDatasets() {
  return useQuery({ queryKey: ["datasets"], queryFn: api.datasets });
}

export function usePreview(dataset: string, offset = 0, limit = 25) {
  return useQuery({
    queryKey: ["preview", dataset, offset, limit],
    queryFn: () => api.preview(dataset, offset, limit),
    enabled: Boolean(dataset),
  });
}

export function useQuality() {
  return useQuery({ queryKey: ["quality"], queryFn: api.quality });
}

export function useReconciliation() {
  return useQuery({ queryKey: ["reconciliation"], queryFn: api.reconciliation });
}

export function useEnrichment(offset = 0, limit = 25) {
  return useQuery({
    queryKey: ["enrichment", offset, limit],
    queryFn: () => api.enrichment(offset, limit),
  });
}

export function useEnrichmentSummary() {
  return useQuery({
    queryKey: ["enrichment-summary"],
    queryFn: api.enrichmentSummary,
  });
}

export function useAnomalies() {
  return useQuery({
    queryKey: ["anomalies"],
    queryFn: api.anomalies,
    refetchInterval: 10_000,
  });
}

export function useLineage() {
  return useQuery({
    queryKey: ["lineage"],
    queryFn: api.lineage,
    refetchInterval: 12_000,
  });
}
