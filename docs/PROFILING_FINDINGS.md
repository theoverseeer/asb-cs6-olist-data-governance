# Profiling findings

Fill this in after running `notebooks/02_profile_sources.py`. Every rule in `DQ_RULES.md` should trace back to a finding here
(or be justified by a business definition). Add your real counts; do not copy numbers from anywhere else.

| # | Table | Finding (from notebook 02) | Evidence (count / %) | Rule created | Decision (quarantine / flag / accept) | Rationale |
|---|---|---|---|---|---|---|
| 1 | orders | Delivered orders without a delivery date | | DQ-ORD-07 | | |
| 2 | order_items | Non-positive price / negative freight | | DQ-ITM-08, DQ-ITM-09 | | |
| 3 | payments | Negative or zero payment value; `not_defined` payment type; 0 installments | | DQ-PAY-04..08 | | |
| 4 | reviews | Same `review_id` on several orders; several reviews per order | | DQ-REV-02, DQ-REV-07 | | |
| 5 | products | Missing / untranslated category; zero weight or dimensions | | DQ-PRD-03..05 | | |
| 6 | geolocation | Many rows per zip (by design); coordinates outside Brazil | | DQ-GEO-03 | | |
| 7 | orders vs items vs payments | Orders whose payment total differs from items + freight | | DQ-ORD-14 | | |
| 8 | all | Orphan keys (orders→customers, items→orders/products/sellers, …) | | DQ-*-03/04/05 integrity rules | | |
