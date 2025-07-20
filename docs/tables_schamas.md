erDiagram
    %% Bronze Layer - Raw Data Ingestion
    bronze_pos_transaction_events {
        string transaction_id PK
        timestamp event_ts
        string product_id
        int quantity_purchased
        double unit_price_at_sale
        string customer_id
        timestamp ingestion_ts
    }
    
    bronze_ticket_booking_events {
        string booking_id PK
        timestamp event_ts
        string screening_id
        int quantity_purchased
        double unit_price_at_sale
        string customer_id
        timestamp ingestion_ts
    }
    
    bronze_supplier_delivery_events {
        string delivery_id PK
        timestamp event_ts
        string product_id
        int quantity_delivered
        double unit_cost
        string supplier_id
        timestamp ingestion_ts
    }
    
    bronze_customer_registration_events {
        string customer_id PK
        timestamp registration_ts
        string name
        string email
        timestamp ingestion_ts
    }
    
    bronze_anime_broadcast_schedule {
        string anime_id PK
        string title
        string genre
        string studio
        timestamp broadcast_start_ts
        timestamp broadcast_end_ts
        string season
        timestamp ingestion_ts
    }
    
    bronze_concession_purchase_events {
        string purchase_id PK
        string screening_id
        int item_quantity
        double item_unit_price
        timestamp ingestion_ts
    }
    
    %% Silver Layer - Cleaned & Enriched Data
    silver_pos_transactions_clean {
        string transaction_id PK
        timestamp transaction_ts
        string customer_id FK
        string product_id FK
        int quantity_purchased
        double total_price
        string linked_anime_id FK
    }
    
    silver_ticket_bookings_clean {
        string booking_id PK
        timestamp booking_ts
        string customer_id FK
        string screening_id
        int quantity_purchased
        double total_price
        string linked_anime_id FK
    }
    
    silver_product_master_data {
        string product_id PK
        string supplier_id
        double current_unit_cost
        double current_unit_price
        timestamp last_cost_update
        timestamp last_price_update
        string linked_anime_id FK
        string category
    }
    
    silver_customer_master_data {
        string customer_id PK
        string name
        string email
    }
    
    silver_inventory_movements_clean {
        string product_id PK
        string linked_anime_id FK
        int current_quantity
        timestamp last_update_ts
        string supplier_id
    }
    
    silver_anime_broadcast_schedule_scd2 {
        string anime_id PK
        string title
        string genre
        string studio
        timestamp broadcast_start_ts
        timestamp broadcast_end_ts
        string season
        timestamp effective_start_date
        timestamp effective_end_date
        boolean is_current_broadcast
    }
    
    silver_concession_sales_summary {
        string screening_id PK
        int total_quantity_purchased
        double total_revenue
    }
    
    %% Gold Layer - Analytics Ready
    fact_sales {
        string sale_id PK
        string customer_id FK
        string product_id FK
        date calendar_date
        int quantity
        double revenue
        int inventory_level_at_sale
    }
    
    fact_screenings {
        string screening_id PK
        string anime_id FK
        date calendar_date
        double ticket_revenue
        double concession_revenue
    }
    
    dim_customer {
        string customer_id PK
        string name
        string email
        string segment FK
    }
    
    customer_segments {
        string segment PK
        int customer_count
        string description
    }
    
    %% Bronze to Silver Relationships
    bronze_pos_transaction_events ||--o{ silver_pos_transactions_clean : "cleaned_from"
    bronze_ticket_booking_events ||--o{ silver_ticket_bookings_clean : "cleaned_from"
    bronze_supplier_delivery_events ||--o{ silver_product_master_data : "enriches"
    bronze_supplier_delivery_events ||--o{ silver_inventory_movements_clean : "calculates"
    bronze_customer_registration_events ||--o{ silver_customer_master_data : "cleaned_from"
    bronze_anime_broadcast_schedule ||--o{ silver_anime_broadcast_schedule_scd2 : "scd2_from"
    bronze_concession_purchase_events ||--o{ silver_concession_sales_summary : "aggregated_from"
    
    %% Silver Layer Relationships
    silver_customer_master_data ||--o{ silver_pos_transactions_clean : "customer_id"
    silver_customer_master_data ||--o{ silver_ticket_bookings_clean : "customer_id"
    silver_product_master_data ||--o{ silver_pos_transactions_clean : "product_id"
    silver_product_master_data ||--o{ silver_inventory_movements_clean : "product_id"
    silver_anime_broadcast_schedule_scd2 ||--o{ silver_pos_transactions_clean : "linked_anime_id"
    silver_anime_broadcast_schedule_scd2 ||--o{ silver_ticket_bookings_clean : "linked_anime_id"
    silver_anime_broadcast_schedule_scd2 ||--o{ silver_product_master_data : "linked_anime_id"
    silver_anime_broadcast_schedule_scd2 ||--o{ silver_inventory_movements_clean : "linked_anime_id"
    
    %% Silver to Gold Relationships
    silver_pos_transactions_clean ||--o{ fact_sales : "aggregated_from"
    silver_ticket_bookings_clean ||--o{ fact_screenings : "ticket_revenue_from"
    silver_concession_sales_summary ||--o{ fact_screenings : "concession_revenue_from"
    silver_customer_master_data ||--o{ dim_customer : "enriched_from"
    silver_anime_broadcast_schedule_scd2 ||--o{ fact_screenings : "anime_id"
    
    %% Gold Layer Relationships
    dim_customer ||--o{ fact_sales : "customer_id"
    silver_product_master_data ||--o{ fact_sales : "product_id"
    silver_anime_broadcast_schedule_scd2 ||--o{ fact_screenings : "anime_id"
    customer_segments ||--o{ dim_customer : "segment"