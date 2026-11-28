const { useState, useEffect, useCallback } = React;
const e = React.createElement;

function pricePillClass(status) {
  if (status === "High Price") return "pill pill-high";
  if (status === "Medium Price") return "pill pill-medium";
  return "pill pill-low";
}

function stockPillClass(status) {
  if (status === "Low Stock") return "pill pill-low-stock";
  if (status === "Medium Stock") return "pill pill-medium";
  return "pill pill-avail";
}

function App() {
  const [health, setHealth] = useState({ status: "connecting", events_count: 0, alerts_count: 0 });
  const [commodities, setCommodities] = useState([]);
  const [alerts, setAlerts] = useState([]);
  const [telemetry, setTelemetry] = useState([]);
  const [containerFilter, setContainerFilter] = useState("");
  const [error, setError] = useState("");

  const refreshAll = useCallback(async () => {
    try {
      const telQuery = containerFilter ? `?limit=25&container_id=${encodeURIComponent(containerFilter)}` : "?limit=25";
      const [hRes, cRes, aRes, tRes] = await Promise.all([
        fetch("/health"),
        fetch("/api/commodities"),
        fetch("/api/alerts?limit=10"),
        fetch(`/api/telemetry${telQuery}`),
      ]);
      if (!hRes.ok) throw new Error(`Bridge HTTP ${hRes.status}`);
      const [hData, cData, aData, tData] = await Promise.all([
        hRes.json(),
        cRes.json(),
        aRes.json(),
        tRes.json(),
      ]);
      setHealth(hData);
      setCommodities(cData.commodities || []);
      setAlerts(aData.alerts || []);
      setTelemetry(tData.events || []);
      setError("");
    } catch (err) {
      setHealth((prev) => ({ ...prev, status: "offline" }));
      setError(err.message || "Unable to reach REST bridge");
    }
  }, [containerFilter]);

  useEffect(() => {
    refreshAll();
    const timer = setInterval(refreshAll, 5000);
    return () => clearInterval(timer);
  }, [refreshAll]);

  const lowStockCount = commodities.filter((c) => c.STOCK_STATUS === "Low Stock").length;
  const connected = health.status === "ok";

  return e(
    "div",
    { className: "container" },
    e(
      "div",
      { className: "header" },
      e(
        "div",
        { className: "title" },
        e("h1", null, "AtmoSync Cold-Chain & Commodity Dashboard"),
        e("p", null, "Live Kafka -> SQLite REST Bridge -> dbt fct_commodity_condition Analytics")
      ),
      e(
        "div",
        { className: "controls" },
        e(
          "span",
          { className: connected ? "badge badge-ok" : "badge badge-err" },
          connected ? "REST Bridge Connected" : `Offline (${error || "Start telemetry_sink.py --serve"})`
        ),
        e("button", { onClick: refreshAll }, "Refresh Now")
      )
    ),
    e(
      "div",
      { className: "kpi-grid" },
      e("div", { className: "card" }, e("div", { className: "kpi-label" }, "Persisted Events"), e("div", { className: "kpi-value" }, health.events_count)),
      e("div", { className: "card" }, e("div", { className: "kpi-label" }, "Anomaly Alerts"), e("div", { className: "kpi-value" }, health.alerts_count)),
      e("div", { className: "card" }, e("div", { className: "kpi-label" }, "Active Commodities"), e("div", { className: "kpi-value" }, commodities.length)),
      e("div", { className: "card" }, e("div", { className: "kpi-label" }, "Low Stock Alerts"), e("div", { className: "kpi-value" }, lowStockCount))
    ),
    e(
      "div",
      { className: "section-grid" },
      e(
        "div",
        { className: "card" },
        e("h2", null, "Commodity Condition (dbt fct_commodity_condition)"),
        e(
          "table",
          null,
          e(
            "thead",
            null,
            e(
              "tr",
              null,
              e("th", null, "Commodity ID"),
              e("th", null, "Name"),
              e("th", null, "Price"),
              e("th", null, "Price Status"),
              e("th", null, "Qty (kg)"),
              e("th", null, "Stock Status"),
              e("th", null, "Temp (C)")
            )
          ),
          e(
            "tbody",
            null,
            commodities.map((row) =>
              e(
                "tr",
                { key: row.COMMODITY_ID },
                e("td", null, row.COMMODITY_ID),
                e("td", null, row.COMMODITY_NAME),
                e("td", null, `$${Number(row.PRICE).toFixed(2)}`),
                e("td", null, e("span", { className: pricePillClass(row.PRICE_STATUS) }, row.PRICE_STATUS)),
                e("td", null, row.QUANTITY),
                e("td", null, e("span", { className: stockPillClass(row.STOCK_STATUS) }, row.STOCK_STATUS)),
                e("td", null, `${row.TEMPERATURE_C}?C`)
              )
            )
          )
        )
      ),
      e(
        "div",
        { className: "card" },
        e("h2", null, "Recent Anomaly Alerts"),
        alerts.length === 0
          ? e("p", { className: "muted" }, "No anomalies recorded.")
          : alerts.map((a) =>
              e(
                "div",
                { className: "alert-item", key: a.event_id },
                e("strong", null, `${a.sensor_status}: ${a.anomaly_type}`),
                e("div", null, `${a.container_id} ? ${a.commodity} (${a.temperature_c}?C)`),
                e("div", { className: "muted" }, `${a.event_id} ? ${a.timestamp_utc}`)
              )
            )
      )
    ),
    e(
      "div",
      { className: "card" },
      e(
        "h2",
        null,
        e("span", null, "Live Container Telemetry Stream"),
        e(
          "select",
          { value: containerFilter, onChange: (ev) => setContainerFilter(ev.target.value) },
          e("option", { value: "" }, "All Containers"),
          ["C001", "C002", "C003", "C004", "C005"].map((cid) => e("option", { key: cid, value: cid }, cid))
        )
      ),
      e(
        "table",
        null,
        e(
          "thead",
          null,
          e(
            "tr",
            null,
            e("th", null, "Event ID"),
            e("th", null, "Container"),
            e("th", null, "Commodity"),
            e("th", null, "Route"),
            e("th", null, "Temp / Humidity"),
            e("th", null, "Status")
          )
        ),
        e(
          "tbody",
          null,
          telemetry.map((ev) =>
            e(
              "tr",
              { key: ev.event_id },
              e("td", null, ev.event_id),
              e("td", null, ev.container_id),
              e("td", null, ev.commodity),
              e("td", null, `${ev.origin} -> ${ev.destination}`),
              e("td", null, `${ev.temperature_c}?C / ${ev.humidity_pct}%`),
              e(
                "td",
                null,
                e("span", { className: ev.sensor_status === "NORMAL" ? "pill pill-normal" : "pill pill-alert" }, ev.sensor_status)
              )
            )
          )
        )
      )
    )
  );
}

ReactDOM.createRoot(document.getElementById("root")).render(e(App));
