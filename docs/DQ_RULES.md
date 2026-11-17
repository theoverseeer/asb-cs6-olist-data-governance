# Data quality rules

**60 rules** over 9 source tables: **47 QUARANTINE** (record is isolated) and **13 FLAG** (record continues, warning is logged).
Dimensions covered: Completeness (12), Consistency (8), Integrity (9), Reasonability (8), Uniqueness (9), Validity (14).

**Failure actions**

* `QUARANTINE` - the record never reaches `valid.*`; it is stored in `quarantine.records` with `failed_rules`, `failed_dimensions`, `failed_rule_count` and the original row as JSON.
* `FLAG` - the record stays in `valid.*` with the rule id in `dq_warning_rules`; used where the defect does not invalidate the business fact.
* No value is auto-corrected. The only transformations are trimming whitespace, turning empty strings into NULL, and type casting (`try_cast`); anything that fails to cast is caught by a rule.

**Why Timeliness and Accuracy are not implemented:** the Olist extract is a static historical snapshot (no freshness SLA applies), and there is no independent ground truth to measure accuracy against. Both are documented as known limitations.

The executable pass-conditions (Spark SQL) live in `notebooks/03_dq_rule_registry.py` / table `governance.dq_rule_registry`; this file is generated from the same list.

## `customers`  ·  domain: Customer  ·  owner: Head of Customer Operations  ·  steward: Customer Data Steward

| Rule ID | Dimension | Table / Field | Rule (condition that must hold) | Business rationale | Failure action | Target pass rate |
|---|---|---|---|---|---|---|
| DQ-CUS-01 | Completeness | `customers`.customer_id, customer_unique_id | customer_id and customer_unique_id must be present. | customer_id is the join key to orders; customer_unique_id is used to count real (repeat) customers. | QUARANTINE | 100% |
| DQ-CUS-02 | Uniqueness | `customers`.customer_id | customer_id must appear once (first occurrence kept, extra copies quarantined). | Duplicate keys fan out joins and double-count revenue. | QUARANTINE | 100% |
| DQ-CUS-03 | Validity | `customers`.customer_state | customer_state must be one of the 27 Brazilian UF codes. | State is a core geographic dimension; invalid codes break regional KPIs and maps. | QUARANTINE | 99.9% |
| DQ-CUS-04 | Validity | `customers`.customer_zip_code_prefix | customer_zip_code_prefix must be exactly 5 digits (read as text, so leading zeros survive). | Zip prefix is the join key to geolocation; malformed values cannot be mapped. | QUARANTINE | 99.9% |
| DQ-CUS-05 | Integrity | `customers`.customer_zip_code_prefix -> geolocation | customer_zip_code_prefix should exist in valid geolocation reference data. | Only affects map visuals, not sales correctness, so the record is flagged and kept. | FLAG | 98% |

## `sellers`  ·  domain: Seller  ·  owner: Head of Seller Success  ·  steward: Seller Data Steward

| Rule ID | Dimension | Table / Field | Rule (condition that must hold) | Business rationale | Failure action | Target pass rate |
|---|---|---|---|---|---|---|
| DQ-SEL-01 | Completeness | `sellers`.seller_id | seller_id must be present. | Seller performance cannot be attributed without a seller key. | QUARANTINE | 100% |
| DQ-SEL-02 | Uniqueness | `sellers`.seller_id | seller_id must appear once. | Duplicate sellers double-count seller revenue. | QUARANTINE | 100% |
| DQ-SEL-03 | Validity | `sellers`.seller_state | seller_state must be a valid UF code. | Seller geography drives regional supply analysis. | QUARANTINE | 99.9% |
| DQ-SEL-04 | Validity | `sellers`.seller_zip_code_prefix | seller_zip_code_prefix must be exactly 5 digits. | Needed to map sellers to geography. | QUARANTINE | 99.9% |

## `products`  ·  domain: Product  ·  owner: Head of Merchandising  ·  steward: Product Data Steward

| Rule ID | Dimension | Table / Field | Rule (condition that must hold) | Business rationale | Failure action | Target pass rate |
|---|---|---|---|---|---|---|
| DQ-PRD-01 | Completeness | `products`.product_id | product_id must be present. | Product key is required to analyse product and category performance. | QUARANTINE | 100% |
| DQ-PRD-02 | Uniqueness | `products`.product_id | product_id must appear once. | Duplicate products double-count item revenue. | QUARANTINE | 100% |
| DQ-PRD-03 | Completeness | `products`.product_category_name | Product should have a category. | Missing category hides revenue under 'unknown'; sales are still real, so flag and keep. | FLAG | 98% |
| DQ-PRD-04 | Integrity | `products`.product_category_name -> category_translation | When present, the category must exist in the translation reference table. | Untranslated categories show Portuguese labels in the dashboard. | FLAG | 99% |
| DQ-PRD-05 | Reasonability | `products`.product_weight_g, length/height/width_cm | Weight and all three dimensions must be greater than zero. | A physical product cannot weigh 0 g or have 0 cm dimensions; affects freight analysis only, so flag and keep. | FLAG | 99% |

## `orders`  ·  domain: Sales  ·  owner: Head of Sales Operations  ·  steward: Sales Data Steward

| Rule ID | Dimension | Table / Field | Rule (condition that must hold) | Business rationale | Failure action | Target pass rate |
|---|---|---|---|---|---|---|
| DQ-ORD-01 | Completeness | `orders`.order_id, customer_id, order_status, order_purchase_timestamp | Order key, customer key, status and purchase timestamp must be present. | These fields anchor every order, customer and time-based metric. | QUARANTINE | 100% |
| DQ-ORD-02 | Uniqueness | `orders`.order_id | order_id must appear once. | Duplicate orders inflate order counts and revenue. | QUARANTINE | 100% |
| DQ-ORD-03 | Integrity | `orders`.customer_id -> valid customers | customer_id must exist in the valid customers table. | An order with no valid customer cannot be attributed to a customer or region. | QUARANTINE | 99.9% |
| DQ-ORD-04 | Validity | `orders`.order_status | order_status must be one of the 8 known lifecycle statuses. | Unknown statuses make revenue/delivery filters unreliable. | QUARANTINE | 100% |
| DQ-ORD-05 | Validity | `orders`.all order timestamps | Every timestamp that is filled in must be a parseable date-time. | Unparseable dates silently become NULL downstream and corrupt delivery metrics. | QUARANTINE | 99.9% |
| DQ-ORD-06 | Reasonability | `orders`.order_purchase_timestamp | Purchase date must be between 2016-01-01 and now. | Orders before the marketplace existed or in the future are impossible (lower bound is a stated assumption; confirm with the Data Owner). | QUARANTINE | 99.9% |
| DQ-ORD-07 | Completeness | `orders`.order_delivered_customer_date, order_approved_at | A delivered order must have both an approval date and a customer delivery date. | A delivered order without delivery date breaks delivery-time and on-time KPIs. | QUARANTINE | 99.9% |
| DQ-ORD-08 | Consistency | `orders`.order_approved_at vs order_purchase_timestamp | Approval cannot happen before purchase. | Time travel in the order lifecycle signals a bad timestamp. | QUARANTINE | 99.9% |
| DQ-ORD-09 | Consistency | `orders`.order_delivered_customer_date vs order_purchase_timestamp | Customer delivery cannot happen before purchase. | Negative delivery times would corrupt average delivery time (same spirit as the negative-fare example in the demo). | QUARANTINE | 99.9% |
| DQ-ORD-10 | Consistency | `orders`.carrier date sequence | Carrier handover must not precede purchase, and customer delivery must not precede carrier handover. | Logistics timestamps are often entered manually; flagged for the steward rather than dropping otherwise-good orders. | FLAG | 99% |
| DQ-ORD-11 | Consistency | `orders`.order_delivered_customer_date vs order_status | Only delivered orders should carry a customer delivery date. | Status and dates contradict each other; needs investigation but revenue logic uses status. | FLAG | 99.5% |
| DQ-ORD-12 | Consistency | `orders`.order_estimated_delivery_date | Estimated delivery date must exist and not precede purchase. | On-time delivery is measured against this promise. | FLAG | 99.5% |
| DQ-ORD-13 | Integrity | `orders`.order_id -> order_items | A delivered order must have at least one order-item record. | A delivered order with no items has no sales value (canceled/unavailable orders may legitimately have none). | QUARANTINE | 99.9% |
| DQ-ORD-14 | Consistency | `orders`.payments vs items total | Total payment value should match items + freight within max(1.00 BRL, 5%). | Cross-dataset reconciliation (same idea as 'CRM vs billing' in the lecture). Installment interest explains small gaps, so tolerance is a steward-owned assumption. | FLAG | 98% |

## `order_items`  ·  domain: Sales  ·  owner: Head of Sales Operations  ·  steward: Sales Data Steward

| Rule ID | Dimension | Table / Field | Rule (condition that must hold) | Business rationale | Failure action | Target pass rate |
|---|---|---|---|---|---|---|
| DQ-ITM-01 | Completeness | `order_items`.order_id, order_item_id, product_id, seller_id | All four key columns must be present. | The item row is the grain of the sales fact; every key is needed to join dimensions. | QUARANTINE | 100% |
| DQ-ITM-02 | Uniqueness | `order_items`.(order_id, order_item_id) | The composite key must appear once. | Duplicate items double-count revenue. | QUARANTINE | 100% |
| DQ-ITM-03 | Validity | `order_items`.order_item_id | order_item_id must be 1 or greater. | Item sequence starts at 1. | QUARANTINE | 99.9% |
| DQ-ITM-04 | Integrity | `order_items`.order_id -> valid orders | Parent order must be a valid order. | Prevents orphan items and cascades order-level failures so facts stay consistent. | QUARANTINE | 99.9% |
| DQ-ITM-05 | Integrity | `order_items`.product_id -> valid products | Product must be a valid product. | Item cannot be attributed to a product/category otherwise. | QUARANTINE | 99.9% |
| DQ-ITM-06 | Integrity | `order_items`.seller_id -> valid sellers | Seller must be a valid seller. | Item cannot be attributed to a seller otherwise. | QUARANTINE | 99.9% |
| DQ-ITM-07 | Validity | `order_items`.price, freight_value | price and freight_value must be present and numeric. | Revenue is computed from these two columns. | QUARANTINE | 100% |
| DQ-ITM-08 | Reasonability | `order_items`.price | price must be greater than 0 and at most 10,000 BRL. | Customers do not pay negative or zero prices (the negative-fare lesson from the demo). Upper bound is a steward-owned assumption. | QUARANTINE | 99.9% |
| DQ-ITM-09 | Reasonability | `order_items`.freight_value | freight_value must be between 0 and 1,000 BRL. | Free shipping (0) is legitimate; negative or extreme freight is not. | QUARANTINE | 99.9% |
| DQ-ITM-10 | Consistency | `order_items`.shipping_limit_date vs order purchase | When present, the seller shipping deadline must not precede order purchase. | Does not affect revenue; flagged for seller-operations review. | FLAG | 99.5% |

## `payments`  ·  domain: Finance  ·  owner: Finance Director  ·  steward: Finance Data Steward

| Rule ID | Dimension | Table / Field | Rule (condition that must hold) | Business rationale | Failure action | Target pass rate |
|---|---|---|---|---|---|---|
| DQ-PAY-01 | Completeness | `payments`.order_id, payment_sequential | Both key columns must be present. | Payment rows must be traceable to an order. | QUARANTINE | 100% |
| DQ-PAY-02 | Uniqueness | `payments`.(order_id, payment_sequential) | The composite key must appear once. | Duplicate payments double-count collected money. | QUARANTINE | 100% |
| DQ-PAY-03 | Integrity | `payments`.order_id -> valid orders | Parent order must be a valid order. | Payments for rejected orders must not reach the analytics layer. | QUARANTINE | 99.9% |
| DQ-PAY-04 | Validity | `payments`.payment_type | payment_type must be credit_card, boleto, voucher or debit_card. | Anything else (e.g. 'not_defined') cannot be analysed in the payment-mix report. | QUARANTINE | 99.9% |
| DQ-PAY-05 | Validity | `payments`.payment_value | payment_value must be present and numeric. | Finance metrics rely on this value. | QUARANTINE | 100% |
| DQ-PAY-06 | Reasonability | `payments`.payment_value | payment_value must not be negative. | Customers do not pay negative amounts (negative-fare lesson). | QUARANTINE | 100% |
| DQ-PAY-07 | Reasonability | `payments`.payment_value | payment_value should be greater than zero. | Zero-value payments can be legitimate (e.g. full voucher), so flag for the steward and keep. | FLAG | 99.5% |
| DQ-PAY-08 | Validity | `payments`.payment_installments | payment_installments must be 1 or greater. | A payment cannot be split into zero installments. | QUARANTINE | 99.9% |
| DQ-PAY-09 | Reasonability | `payments`.payment_installments | payment_installments should not exceed 24. | Unusually long plans need confirmation (24 is an assumption; confirm with Finance). | FLAG | 99.5% |

## `reviews`  ·  domain: Customer Experience  ·  owner: Head of Customer Experience  ·  steward: CX Data Steward

| Rule ID | Dimension | Table / Field | Rule (condition that must hold) | Business rationale | Failure action | Target pass rate |
|---|---|---|---|---|---|---|
| DQ-REV-01 | Completeness | `reviews`.review_id, order_id | Both key columns must be present. | A review must be traceable to an order. | QUARANTINE | 100% |
| DQ-REV-02 | Uniqueness | `reviews`.(review_id, order_id) | The pair must appear once (the same review_id legitimately appears on different orders). | Exact duplicates double-weight satisfaction scores. | QUARANTINE | 100% |
| DQ-REV-03 | Integrity | `reviews`.order_id -> valid orders | Parent order must be a valid order. | Reviews of rejected orders must not reach the analytics layer. | QUARANTINE | 99.9% |
| DQ-REV-04 | Validity | `reviews`.review_score | review_score must be an integer from 1 to 5. | Satisfaction scale is 1-5; other values distort averages. | QUARANTINE | 100% |
| DQ-REV-05 | Completeness | `reviews`.review_creation_date | review_creation_date must be present and parseable. | Needed to place the review in time. | QUARANTINE | 100% |
| DQ-REV-06 | Consistency | `reviews`.review_answer_timestamp vs review_creation_date | Review answer cannot precede review creation. | Affects response-time metric only. | FLAG | 99.5% |
| DQ-REV-07 | Uniqueness | `reviews`.order_id | Each order should have only one review. | Multiple reviews per order complicate satisfaction-per-order metrics; flagged, not removed. | FLAG | 99% |

## `geolocation`  ·  domain: Reference Data  ·  owner: Data Governance Lead  ·  steward: Reference Data Steward

| Rule ID | Dimension | Table / Field | Rule (condition that must hold) | Business rationale | Failure action | Target pass rate |
|---|---|---|---|---|---|---|
| DQ-GEO-01 | Completeness | `geolocation`.zip, lat, lng | Zip prefix, latitude and longitude must be present. | A coordinate row without a zip or coordinates is useless. | QUARANTINE | 99.9% |
| DQ-GEO-02 | Validity | `geolocation`.geolocation_zip_code_prefix | Zip prefix must be exactly 5 digits. | Join key to customers/sellers. | QUARANTINE | 99.9% |
| DQ-GEO-03 | Reasonability | `geolocation`.geolocation_lat, geolocation_lng | Coordinates must fall inside Brazil's bounding box (lat -34 to 5.5, lng -74 to -34). | Points outside Brazil would distort maps and averaged coordinates. | QUARANTINE | 99.5% |
| DQ-GEO-04 | Validity | `geolocation`.geolocation_state | State must be a valid UF code. | Consistent state codes across all reference data. | QUARANTINE | 99.9% |

## `category_translation`  ·  domain: Reference Data  ·  owner: Data Governance Lead  ·  steward: Reference Data Steward

| Rule ID | Dimension | Table / Field | Rule (condition that must hold) | Business rationale | Failure action | Target pass rate |
|---|---|---|---|---|---|---|
| DQ-CAT-01 | Completeness | `category_translation`.both columns | Portuguese and English names must be present. | A half-filled mapping row cannot translate anything. | QUARANTINE | 100% |
| DQ-CAT-02 | Uniqueness | `category_translation`.product_category_name | Each Portuguese category appears once. | Duplicates would fan out the product dimension. | QUARANTINE | 100% |
