# Profiling findings

Evidence from `notebooks/02_profile_sources.py` and the follow-up summary query, run in Databricks on the raw layer (`olist_gov.raw.*`).
Row counts: customers 99,441 · sellers 3,095 · products 32,951 · orders 99,441 · order_items 112,650 · payments 103,886 · reviews 99,224 · geolocation 1,000,163 · category_translation 71.

| # | Table | Finding | Evidence (records) | Rule | Decision | Rationale |
|---|---|---|---|---|---|---|
| 1 | orders | Delivered orders with no customer delivery date | 8 | DQ-ORD-07 | Quarantine | Delivery time and on-time KPIs cannot be computed |
| 2 | orders | Delivered orders with no approval date | 14 | DQ-ORD-07 | Quarantine | A delivered order must have passed approval; lifecycle is contradictory |
| 3 | orders | Non-delivered orders that carry a delivery date | 6 | DQ-ORD-11 | Flag | Status and date contradict each other, but revenue logic uses status |
| 4 | orders | Approved before purchase / delivered before purchase | 0 / 0 | DQ-ORD-08, DQ-ORD-09 | Quarantine (preventive) | None found; kept so future loads cannot introduce time travel |
| 5 | orders | Delivered orders with no item rows | 0 | DQ-ORD-13 | Quarantine (preventive) | None found; a delivered order without items has no sales value |
| 6 | order_items | Price <= 0, negative freight | 0 / 0 | DQ-ITM-08, DQ-ITM-09 | Quarantine (preventive) | None found in the file; the "negative fare" lesson, proven by notebook 10 |
| 7 | payments | Negative payment value | 0 | DQ-PAY-06 | Quarantine (preventive) | None found; customers do not pay negative amounts |
| 8 | payments | Zero-value payments | 9 | DQ-PAY-07 | Flag | Can be legitimate (e.g. full voucher); steward review |
| 9 | payments | payment_type = not_defined | 3 | DQ-PAY-04 | Quarantine | Cannot be analysed in the payment-mix report |
| 10 | payments | Installments below 1 | 2 | DQ-PAY-08 | Quarantine | A payment cannot have zero installments |
| 11 | reviews | Orders with more than one review | 547 | DQ-REV-07 | Flag | Latest review is used for scores; the rest are kept, not deleted |
| 12 | products | Products without a category | 610 | DQ-PRD-03 | Flag | Sales are real; revenue shows under "unknown" |
| 13 | products | Products with an untranslated category | 13 | DQ-PRD-04 | Flag | Portuguese label shown instead of English |
| 14 | geolocation | Points outside Brazil's bounding box | 42 | DQ-GEO-03 | Quarantine | Would distort maps and averaged coordinates |
| 15 | all | Duplicate business keys (orders, items) | 0 | DQ-*-02 uniqueness rules | Quarantine (preventive) | Clean in this extract; kept as a control for reloads |
| 16 | all | Orphan keys (items, payments, reviews with no order) | 0 | DQ-ITM-04, DQ-PAY-03, DQ-REV-03 | Quarantine (cascade) | Clean at source; these rules also cascade from rejected orders |
