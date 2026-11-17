# Analytical data model

Schema: `<catalog>.analytics`. Built **only** from `valid.*` tables. Surrogate keys are `xxhash64(business key)`.

## Entity-relationship diagram

```mermaid
erDiagram
    dim_date        ||--o{ fact_order_items : purchase_date_key
    dim_date        ||--o{ fact_orders      : purchase_date_key
    dim_date        ||--o{ fact_reviews     : review_date_key
    dim_customer    ||--o{ fact_order_items : customer_key
    dim_customer    ||--o{ fact_orders      : customer_key
    dim_customer    ||--o{ fact_payments    : customer_key
    dim_customer    ||--o{ fact_reviews     : customer_key
    dim_product     ||--o{ fact_order_items : product_key
    dim_seller      ||--o{ fact_order_items : seller_key
    fact_orders     ||--o{ fact_order_items : order_id
    fact_orders     ||--o{ fact_payments    : order_id
    fact_orders     ||--o{ fact_reviews     : order_id
    dim_geography   ||..o{ dim_customer     : "zip_code_prefix (reference)"
    dim_geography   ||..o{ dim_seller       : "zip_code_prefix (reference)"

    dim_date { int date_key PK  date date  int year  int month  date month_start  boolean is_weekend }
    dim_customer { bigint customer_key PK  string customer_unique_hash BK  string customer_state  string customer_city  int orders_total  int valid_orders  string customer_type }
    dim_product { bigint product_key PK  string product_id BK  string category_pt  string category_en  double product_weight_g  double volume_cm3 }
    dim_seller { bigint seller_key PK  string seller_id BK  string seller_state  string seller_city }
    dim_geography { bigint geography_key PK  string zip_code_prefix BK  double latitude  double longitude }
    fact_orders { string order_id PK  bigint customer_key FK  int purchase_date_key FK  string order_status  boolean is_valid_order  decimal items_value  decimal order_value  int delivery_days  boolean is_late  int review_score  boolean is_complete_order }
    fact_order_items { string order_id PK  int order_item_id PK  bigint customer_key FK  bigint product_key FK  bigint seller_key FK  int purchase_date_key FK  decimal price  decimal freight_value  double order_weight }
    fact_payments { string order_id PK  int payment_sequential PK  bigint customer_key FK  string payment_type  decimal payment_value }
    fact_reviews { string review_id PK  string order_id PK  bigint customer_key FK  int review_date_key FK  int review_score  boolean is_latest_review }
```

`dim_geography` is joined through the zip prefix (a reference lookup), not through a surrogate key stored on the facts.

## Grain of each fact table

| Fact | Grain (one row per …) | Main measures | Use it for |
|---|---|---|---|
| `fact_orders` | valid order `order_id` | `items_value`, `freight_value`, `order_value`, `delivery_days`, `delay_days`, `is_late`, `review_score` (latest), `payment_value` | order counts, AOV, customers, delivery performance |
| `fact_order_items` | valid order item `(order_id, order_item_id)` | `price`, `freight_value`, `item_value`, `order_weight` | sales by category / seller |
| `fact_payments` | valid payment `(order_id, payment_sequential)` | `payment_value`, `payment_installments` | payment mix |
| `fact_reviews` | valid review on an order `(review_id, order_id)` | `review_score`, `response_hours`, `is_latest_review` | review distribution |

Why two sales-related facts? Order-level metrics (counts, delivery time, review) would be **double-counted** if computed on the item grain, so they live on `fact_orders`. On the item grain, order-level fields are averaged with `order_weight = 1 / items in the order` and order values are never summed - the same rule as in the earlier Case Study 4 model.

## Dimensions

| Dimension | Grain | Notes |
|---|---|---|
| `dim_date` | day | generated from the min/max dates in valid data |
| `dim_customer` | **person** (`customer_unique_id`) | stored as a salted SHA-256 hash; state/city of the latest order; `orders_total`, `valid_orders`, `customer_type` (Repeat / One-time / No valid order). `customer_id` is order-scoped, so it is not used as the customer |
| `dim_product` | `product_id` | `category_en` falls back to the Portuguese name if untranslated, `unknown` if missing (a label, not a correction) |
| `dim_seller` | `seller_id` | |
| `dim_geography` | 5-digit zip prefix | coordinates averaged over valid points; used for reference/maps, not joined to facts |

## Handling of partially-quarantined orders
If one item of an order is quarantined but its siblings are valid, the order stays valid. `fact_orders.is_complete_order = false`
marks it (`raw_item_count <> valid_item_count`), and `semantic.*` exposes the flag so analysts can exclude such orders.
