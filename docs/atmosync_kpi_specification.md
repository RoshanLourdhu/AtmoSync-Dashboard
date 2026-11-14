# AtmoSync – Micro-Climate Arbitrage Analytics

## 1. Project Objective & Dashboard Purpose

**Confirmed Requirement:**
- The AtmoSync platform aggregates real-time sensor data from refrigerated containers (temperature, humidity, vibration) to enable **micro-climate monitoring and commodity spoilage mitigation**.
- The Superset dashboard provides logistics managers, supply-chain analysts, and operations teams with a single pane of glass for **Key Performance Indicators (KPIs)**, risk alerts, and actionable recommendations for container rerouting or expedited dispatch.

---

## 2. KPI Definitions

| KPI | Description | SQL Formula | Status |
| :--- | :--- | :--- | :--- |
| **Avg Temperature** | Mean temperature inside monitored containers. | `AVG(temperature)` | Confirmed |
| **Max Temperature** | Peak temperature recorded across active containers. | `MAX(temperature)` | Confirmed |
| **Avg Humidity** | Mean relative humidity inside monitored containers. | `AVG(humidity)` | Confirmed |
| **Max Humidity** | Peak humidity recorded across active containers. | `MAX(humidity)` | Confirmed |
| **Vibration Index** | Root-mean-square (RMS) vibration acceleration index. | `SQRT(AVG(vibration * vibration))` | Confirmed |
| **At-Risk Container Count** | Number of unique containers classified as WARNING or CRITICAL. | `COUNT(DISTINCT CASE WHEN risk_level IN ('WARNING', 'CRITICAL') THEN container_id END)` | Confirmed |

---

## 3. Spoilage Risk Indicators & Composite Score

### Risk Flag Definitions:
- **Temperature Risk Flag (`temp_risk_flag`):** Evaluated against commodity-specific thresholds (Avocados: >8.0°C, Mangoes: >10.5°C, Bananas: >15.0°C; default: >10.0°C).
- **Humidity Risk Flag (`hum_risk_flag`):** Evaluated against high-humidity threshold (>85.0%).
- **Vibration Spike Flag (`vib_risk_flag`):** Evaluated against transit vibration alert threshold (>0.28 g).

### Composite Spoilage Score:
$$\text{Spoilage Score} = 0.6 \cdot \text{Temp\_Risk} + 0.3 \cdot \text{Hum\_Risk} + 0.1 \cdot \text{Vib\_Risk}$$

- Range: `[0.0, 1.0]`

---

## 4. Container Risk Classification

| Risk Level | Score Threshold | Operational Action |
| :--- | :--- | :--- |
| **CRITICAL** | $\ge 0.70$ | Immediate inspection, expedited dispatch, or cargo liquidation |
| **WARNING** | $0.30 \le \text{Score} < 0.70$ | Review route, verify refrigeration unit, consider alternate transit hub |
| **NORMAL** | $< 0.30$ | Maintain standard transit schedule and monitoring |

---

## 5. Rerouting Opportunities & Commercial Arbitrage

To prevent analytical distortion, monetary price spreads and spot market prices are **not fabricated**.
Containers at elevated risk (`WARNING` or `CRITICAL`) are designated as `is_reroute_candidate = TRUE` in the analytical mart `fct_arbitrage_opportunities`.
Once external market spot price feeds are integrated, net margin differentials will be populated.

---

## 6. Target Dataset Fields

All upstream models conform strictly to the 7-field JSON contract:
- `container_id` (`STRING`)
- `shipment_id` (`STRING`)
- `commodity` (`STRING`)
- `temperature` (`FLOAT`)
- `humidity` (`FLOAT`)
- `vibration` (`FLOAT`)
- `recorded_at` (`TIMESTAMP_TZ`)
