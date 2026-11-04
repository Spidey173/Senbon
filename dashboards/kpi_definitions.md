# Senbon — Enterprise KPI & Metric Catalog

| Metric Name | Business Definition | Technical Formula (SQL) | Target Granularity | Business Owner | SLA / Target |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Gross Merchandise Value (GMV)** | Total sales price of all goods ordered prior to deductions. | `SUM(fct_sales.line_total)` | Daily, Monthly, Store | Finance | N/A |
| **Net Revenue** | Final recognized transaction value after discounts. | `SUM(fct_orders.net_amount)` | Daily, Monthly, Store | Finance | \$15M Annual |
| **Gross Profit Margin (%)** | Percentage of revenue retained after deducting cost of goods sold. | `(SUM(line_profit) / SUM(line_total)) * 100` | Monthly, Category | Merchandising | > 42.0% |
| **Average Order Value (AOV)** | Average dollar amount spent per completed order transaction. | `SUM(net_amount) / COUNT(order_key)` | Daily, Segment | Growth & Marketing | > \$85.00 |
| **Stockout Risk Rate** | Proportion of inventory positions at or below critical reorder point. | `(COUNT(risk_true) / COUNT(total_positions)) * 100` | Daily Snapshot, Store | Supply Chain | < 5.0% |
| **Revenue per Sq Ft** | Physical store sales productivity normalized by retail floor size. | `store_revenue / square_feet` | Quarterly, Store | Retail Operations | > \$450 / sq ft |
| **Late-Arriving Ingestion Rate** | Volume of transactions originating > 30 days prior to batch ingestion. | `(COUNT(late_true) / COUNT(total_orders)) * 100` | Batch Run | Data Platform | < 1.0% |
| **Data Quality Pass Rate (%)** | Percentage of raw ingested records meeting 100% of schema constraints. | `(clean_records / total_ingested_records) * 100` | Batch Run, Entity | Data Platform | > 99.0% |
| **Pipeline Latency SLA** | Total wall-clock time from raw landing drop to star schema availability. | `MAX(warehouse_logged_at) - MIN(raw_ingested_at)` | Batch Execution | Data Platform | < 15 mins |

