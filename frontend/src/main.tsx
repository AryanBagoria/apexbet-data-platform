import React from "react";
import ReactDOM from "react-dom/client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { createBrowserRouter, RouterProvider, Navigate } from "react-router-dom";

import { AppLayout } from "./layouts/AppLayout";
import { OverviewPage } from "./pages/OverviewPage";
import { StreamingPage } from "./pages/StreamingPage";
import { LineagePage } from "./pages/LineagePage";
import { LayersPage } from "./pages/LayersPage";
import { QualityPage } from "./pages/QualityPage";
import { AiPage } from "./pages/AiPage";
import { AnalyticsPage } from "./pages/AnalyticsPage";
import { DemoPage } from "./pages/DemoPage";
import "./styles/global.css";
import "reactflow/dist/style.css";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      retry: 1,
      staleTime: 5_000,
    },
  },
});

const router = createBrowserRouter([
  {
    path: "/",
    element: <AppLayout />,
    children: [
      { index: true, element: <OverviewPage /> },
      { path: "streaming", element: <StreamingPage /> },
      { path: "lineage", element: <LineagePage /> },
      { path: "layers", element: <LayersPage /> },
      { path: "quality", element: <QualityPage /> },
      { path: "ai", element: <AiPage /> },
      { path: "analytics", element: <AnalyticsPage /> },
      { path: "demo", element: <DemoPage /> },
      { path: "*", element: <Navigate to="/" replace /> },
    ],
  },
]);

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>
  </React.StrictMode>,
);
