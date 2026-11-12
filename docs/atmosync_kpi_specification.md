# AtmoSync – Micro‑Climate Arbitrage Analytics
## 1. Project Objective & Dashboard Purpose
**Confirmed Requirement**
- The AtmoSync platform aggregates real‑time sensor data from refrigerated containers (temperature, humidity, vibration) to enable **micro‑climate arbitrage**. 
- The Superset dashboard must provide stakeholders (logistics managers, supply‑chain analysts, finance teams) with a single view of **key performance indicators (KPIs)**, risk alerts, and actionable insights for **rerouting decisions**.

**Proposed Enhancements** (subject to validation)
- Integration with external market price feeds for dynamic pricing models.
- Predictive analytics for spoilage risk based on historical patterns.

---
## 2. KPI Definitions
| KPI | Description | Formula (if applicable) | Status |
|-----|-------------|--------------------------|--------|
| **Avg Temperature** | Mean temperature inside the container over the selected period. | `AVG(temperature)` | Confirmed |
| **Max Temperature** | Highest temperature recorded. | `MAX(temperature)` | Confirmed |
| **Avg Humidity** | Mean humidity level inside the container. | `AVG(humidity)` | Confirmed |
| **Max Humidity** | Highest humidity recorded. | `MAX(humidity)` | Confirmed |
| **Vibration Index** | Aggregated vibration metric (e.g., RMS of acceleration). | `SQRT(AVG(vibration^2))` | Confirmed |
| **Temperature Variance** | Variability of temperature. | `VAR_SAMP(temperature)` | Proposed (pending validation) |
| **Humidity Variance** | Variability of humidity. | `VAR_SAMP(humidity)` | Proposed (pending validation) |

---
## 3. Spoilage Risk Indicators
**Confirmed Requirement**
- **Temperature Risk Flag** – triggers when temperature exceeds a predefined safe limit.
- **Humidity Risk Flag** – triggers when humidity exceeds a safe limit.
- **Vibration Spike Flag** – triggers when vibration index exceeds a safe limit.

**Proposed / Pending Validation**
- Composite **Spoilage Score** = `0.6*Temp_Risk + 0.3*Hum_Risk + 0.1*Vib_Risk` (weights to be validated).
- Thresholds for each component are **pending validation** and should be defined in collaboration with the operations team.

---
## 4. At‑Risk Container Count & Risk Classification
| Classification | Condition (pending validation) | Action |
|----------------|-------------------------------|--------|
| **Critical**   | Any risk flag true AND Spoilage Score ≥ **TODO** | Immediate inspection & possible container quarantine |
| **Warning**    | Any risk flag true AND Spoilage Score between **TODO** and **TODO** | Review & decide on possible reroute |
| **Normal**     | No risk flags | Continue normal monitoring |

- **At‑Risk Container Count** = `COUNT(*)` of containers whose classification is *Critical* or *Warning*.

---
## 5. Rerouting Opportunity & Potential Value Metrics
**Proposed Metrics** (to be validated with finance team):
- **Potential Savings** = `(Baseline transport cost – Optimized transport cost) * Volume`.
- **Revenue Impact** = `Avoided spoilage cost + Savings`.
- **Reroute Feasibility Score** = `Potential Savings / Estimated Reroute Cost`.

All monetary values and cost assumptions are **pending validation**.

---
## 6. Apache Superset Dashboard Layout
| Section | Chart Type | Purpose |
|---------|------------|---------|
| **Overview** | KPI tiles (large numbers) | Show current values for Avg Temp, Avg Humidity, Vibration Index, At‑Risk Container Count |
| **Temperature Trend** | Line chart | Visualize temperature over time per container/group |
| **Humidity Trend** | Line chart | Visualize humidity over time |
| **Vibration Pattern** | Area/Stacked area chart | Show vibration intensity distribution |
| **Risk Matrix** | Heat map | Cross‑tabulate Temperature Risk vs. Humidity Risk |
| **Spoilage Score Distribution** | Histogram | Identify frequency of high‑risk scores |
| **Rerouting Opportunities** | Bar chart (sorted) | List containers with highest feasibility scores |
| **Data Quality** | Table with row counts & missing‑value percentages | Quick health check of source data |

---
## 7. Required Source Columns for Each KPI
| KPI | Required Source Columns |
|-----|--------------------------|
| Avg/Max Temperature | `temperature` (numeric), `timestamp`, `container_id` |
| Avg/Max Humidity | `humidity` (numeric), `timestamp`, `container_id` |
| Vibration Index | `vibration` (numeric), `timestamp`, `container_id` |
| Spoilage Score | `temperature`, `humidity`, `vibration`, optional `sensor_status` |
| At‑Risk Classification | All risk‑related columns above + any manually defined thresholds |

> **Note:** Exact column names in the upstream dataset are **pending validation**. Adjust the mapping once the data schema is finalized.

---
## 8. Data Quality Checks & Missing‑Data Handling
1. **Row‑Level Completeness** – Ensure each record contains non‑null `container_id`, `timestamp`, and at least one sensor reading.
2. **Column‑Level Null Ratio** – Flag columns where null percentage > **TODO** (e.g., 5%).
3. **Timestamp Monotonicity** – Detect out‑of‑order timestamps per container.
4. **Outlier Detection** – Use IQR or Z‑score methods (thresholds **pending validation**) to flag extreme sensor readings.
5. **Imputation Strategy** –
   - If a single sensor value is missing, use forward‑fill within the same container.
   - If multiple sensors are missing for a row, mark the row as **incomplete** and exclude it from KPI calculations (record reason).
6. **Data Freshness** – Alert if the latest timestamp for any container is older than **TODO** minutes.

---
### Revision History
| Date | Author | Change |
|------|--------|--------|
| 2026‑11‑12 | Navya (BI Lead) | Initial specification draft |

*All placeholders marked **TODO** or **pending validation** must be resolved before production deployment.*

