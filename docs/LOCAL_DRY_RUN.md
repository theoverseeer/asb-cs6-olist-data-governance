# Local dry-run results

Produced by `tools/local_dry_run.py` (pandas + SQLite re-implementation of the engine; not Spark).

**Total:** 1,550,922 records evaluated, 1,550,786 valid, 136 quarantined (pass rate 99.991 %).

## Per table

| table                |   evaluated |   valid |   quarantined |   valid_with_warnings |   pass_rate_% |
|:---------------------|------------:|--------:|--------------:|----------------------:|--------------:|
| category_translation |          71 |      71 |             0 |                     0 |       100     |
| geolocation          |     1000163 | 1000121 |            42 |                     0 |        99.996 |
| customers            |       99441 |   99441 |             0 |                   279 |       100     |
| sellers              |        3095 |    3095 |             0 |                     0 |       100     |
| products             |       32951 |   32951 |             0 |                   628 |       100     |
| orders               |       99441 |   99419 |            22 |                   417 |        99.978 |
| order_items          |      112650 |  112627 |            23 |                     0 |        99.98  |
| payments             |      103886 |  103859 |            27 |                     6 |        99.974 |
| reviews              |       99224 |   99202 |            22 |                  1098 |        99.978 |

## Rules that fired

| rule_id   | dimension     | table       | action     |   failed |   pass_rate_% |   target_% | status   | rule                                                                                                 |
|:----------|:--------------|:------------|:-----------|---------:|--------------:|-----------:|:---------|:-----------------------------------------------------------------------------------------------------|
| DQ-REV-07 | Uniqueness    | reviews     | FLAG       |     1098 |        98.893 |       99   | WARNING  | Each order should have only one review.                                                              |
| DQ-PRD-03 | Completeness  | products    | FLAG       |      610 |        98.149 |       98   | PASSED   | Product should have a category.                                                                      |
| DQ-CUS-05 | Integrity     | customers   | FLAG       |      279 |        99.719 |       98   | PASSED   | customer_zip_code_prefix should exist in valid geolocation reference data.                           |
| DQ-ORD-14 | Consistency   | orders      | FLAG       |      222 |        99.777 |       98   | PASSED   | Total payment value should match items + freight within max(1.00 BRL, 5%).                           |
| DQ-ORD-10 | Consistency   | orders      | FLAG       |      189 |        99.81  |       99   | PASSED   | Carrier handover must not precede purchase, and customer delivery must not precede carrier handover. |
| DQ-GEO-03 | Reasonability | geolocation | QUARANTINE |       42 |        99.996 |       99.5 | PASSED   | Coordinates must fall inside Brazil's bounding box (lat -34 to 5.5, lng -74 to -34).                 |
| DQ-ITM-04 | Integrity     | order_items | QUARANTINE |       23 |        99.98  |       99.9 | PASSED   | Parent order must be a valid order.                                                                  |
| DQ-ORD-07 | Completeness  | orders      | QUARANTINE |       22 |        99.978 |       99.9 | PASSED   | A delivered order must have both an approval date and a customer delivery date.                      |
| DQ-REV-03 | Integrity     | reviews     | QUARANTINE |       22 |        99.978 |       99.9 | PASSED   | Parent order must be a valid order.                                                                  |
| DQ-PAY-03 | Integrity     | payments    | QUARANTINE |       22 |        99.979 |       99.9 | PASSED   | Parent order must be a valid order.                                                                  |
| DQ-PRD-04 | Integrity     | products    | FLAG       |       13 |        99.961 |       99   | PASSED   | When present, the category must exist in the translation reference table.                            |
| DQ-PAY-07 | Reasonability | payments    | FLAG       |        9 |        99.991 |       99.5 | PASSED   | payment_value should be greater than zero.                                                           |
| DQ-ORD-11 | Consistency   | orders      | FLAG       |        6 |        99.994 |       99.5 | PASSED   | Only delivered orders should carry a customer delivery date.                                         |
| DQ-PRD-05 | Reasonability | products    | FLAG       |        6 |        99.982 |       99   | PASSED   | Weight and all three dimensions must be greater than zero.                                           |
| DQ-PAY-04 | Validity      | payments    | QUARANTINE |        3 |        99.997 |       99.9 | PASSED   | payment_type must be credit_card, boleto, voucher or debit_card.                                     |
| DQ-PAY-08 | Validity      | payments    | QUARANTINE |        2 |        99.998 |       99.9 | PASSED   | payment_installments must be 1 or greater.                                                           |

## KPI reconciliation

| metric                               |   earlier_dashboard |   recomputed_from_raw_(no_DQ) |   governed_(valid_only) |   raw_vs_earlier_% |   governed_vs_earlier_% |
|:-------------------------------------|--------------------:|------------------------------:|------------------------:|-------------------:|------------------------:|
| Orders (all)                         |     99441           |               99441           |         99419           |             0      |                 -0.0221 |
| Order items                          |    112650           |              112650           |        112627           |             0      |                 -0.0204 |
| Sum of item prices, all orders (BRL) |         1.35916e+07 |                   1.35916e+07 |             1.35887e+07 |             0      |                 -0.0215 |
| Sum of payments (BRL)                |         1.60089e+07 |                   1.60089e+07 |             1.60053e+07 |            -0      |                 -0.022  |
| Valid orders                         |     98207           |               98207           |         98185           |             0      |                 -0.0224 |
| Unique customers (valid orders)      |     94990           |               94990           |         94969           |             0      |                 -0.0221 |
| Sales (Items), valid orders (BRL)    |         1.34944e+07 |                   1.34944e+07 |             1.34915e+07 |             0      |                 -0.0216 |
| AOV (BRL)                            |       160.23        |                 160.228       |           160.23        |            -0.0011 |                  0.0001 |
| Delivered orders with delivered date |     96470           |               96470           |         96456           |             0      |                 -0.0145 |
| Late delivery %                      |         6.77        |                   6.77309     |             6.77407     |             0.0456 |                  0.0602 |
| Avg delivery days                    |        12.5         |                  12.4968      |            12.4967      |            -0.0252 |                 -0.0263 |
| Reviewed orders                      |     98673           |               98673           |         98651           |             0      |                 -0.0223 |
| Avg review score                     |         4.09        |                   4.08639     |             4.08631     |            -0.0884 |                 -0.0901 |
