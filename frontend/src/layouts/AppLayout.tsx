import { NavLink, Outlet } from "react-router-dom";

// Application shell: sidebar navigation + top bar + routed content.

const NAV = [
  { section: "Monitor" },
  { to: "/", label: "Overview", icon: "◎", end: true },
  { to: "/streaming", label: "Event Stream", icon: "≋" },
  { to: "/lineage", label: "Pipeline Lineage", icon: "⛓" },
  { section: "Data" },
  { to: "/layers", label: "Data Pipeline", icon: "▤" },
  { to: "/quality", label: "Data Quality", icon: "✓" },
  { to: "/ai", label: "Enrichment & Anomalies", icon: "✦" },
  { to: "/analytics", label: "Analytics", icon: "▦" },
  { section: "Operate" },
  { to: "/demo", label: "Pipeline Controls", icon: "▶" },
];

export function AppLayout() {
  return (
    <div className="app-shell">
      <aside className="sidebar" aria-label="Primary navigation">
        <div className="brand">
          <div className="brand__mark">A</div>
          <div>
            <div className="brand__name">ApexBet</div>
            <div className="brand__sub">Data Platform</div>
          </div>
        </div>
        <nav className="nav">
          {NAV.map((item, i) =>
            item.section ? (
              <div className="nav__section" key={`s-${i}`}>
                {item.section}
              </div>
            ) : (
              <NavLink
                key={item.to}
                to={item.to!}
                end={item.end}
                className={({ isActive }) =>
                  `nav__link ${isActive ? "nav__link--active" : ""}`
                }
              >
                <span className="nav__icon" aria-hidden="true">
                  {item.icon}
                </span>
                {item.label}
              </NavLink>
            ),
          )}
        </nav>
        <div className="sidebar__foot">
          Local demonstration · synthetic data only. No real customers, accounts,
          or payments.
        </div>
      </aside>

      <header className="topbar">
        <div className="topbar__left">
          <span className="badge-demo">APEXBET — SYNTHETIC DATA DEMONSTRATION</span>
        </div>
        <div className="topbar__right">
          <span>React · TypeScript · FastAPI</span>
          <a href="/docs" target="_blank" rel="noreferrer">
            API docs ↗
          </a>
        </div>
      </header>

      <main className="main">
        <div className="main__inner">
          <Outlet />
        </div>
      </main>
    </div>
  );
}
