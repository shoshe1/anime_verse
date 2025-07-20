## SCD Type 2 Implementation

- Applied to the `silver_anime_broadcast_schedule_scd2` table.
- Detects changes in key attributes (title, genre, studio, broadcast dates, season).
- For any change:
  - Current record is marked with `is_current_broadcast = False` and an `effective_end_date` timestamp.
  - New record is inserted with updated attributes, `is_current_broadcast = True`, and `effective_start_date`.
- This preserves full history of broadcast schedules for analytics.

## SCD Type 2 Implementation for dim_product

- Applied to the dim_product table.
- Tracks changes in key product attributes: name, category, price, cost, and supplier_id.
- For any detected change compared to the current record:
    - The existing current record is marked as no longer current (is_current = False) and given an effective_end_date timestamp to mark when it expired.
    - A new record with updated attribute values is inserted with is_current = True and a new effective_start_date.
        Unchanged current records remain unchanged.

-This approach preserves the full historical timeline of product data, enabling analytics on how product attributes evolved over time.
## Late Arrival Supplier Data

- Supplier delivery events can arrive late and out-of-order.
- Deduplication based on `delivery_id` and timestamp fields.
- Latest records for cost and price per product are selected using window functions ordered by ingestion timestamps.
- The ETL logic accommodates late arrivals by overwriting or appending based on ingestion time to maintain data correctness.