# System Architecture
graph TB

   

    %% Bronze Layer
    subgraph "Bronze Layer (Raw Data)"
        B1[bronze_pos_transaction_events]
        B2[bronze_ticket_booking_events]
        B3[bronze_customer_registration_events]
        B4[bronze_supplier_delivery_events]
        B5[bronze_concession_purchase_events]
        B6[bronze_anime_broadcast_schedule]
    end

    %% Silver Layer
    subgraph "Silver Layer (Cleaned & Transformed)"
        S1[silver_pos_transactions_clean]
        S2[silver_ticket_bookings_clean]
        S3[silver_customer_master_data]
        S4[silver_product_master_data]
        S5[silver_inventory_movements_clean]
        S6[silver_anime_broadcast_schedule_scd2]
        S7[silver_concession_sales_summary]
    end

    %% Gold Layer
    subgraph "Gold Layer (Business Ready)"
        subgraph "Fact Tables"
            G1[fact_sales]
            G2[fact_screenings]
        end
        
        subgraph "Dimension Tables"
            G3[dim_customer]
            G4[dim_product]
            G5[dim_anime_title_scd2]
        end
        
        subgraph "Business Tables"
            G6[customer_segments]
            G7[gold_top_anime_performance_tiles]
            G8[gold_inventory_summary]
        end
        
        subgraph "ML Features"
            G9[ML_features_upcoming_titles]
        end
    end

 

    %% Data Flow - Sources to Bronze
    
    %% ETL Process 1: Bronze to Silver
    B1 --> |Clean & Dedupe| S1
    B2 --> |Clean & Link Anime| S2
    B3 --> |Clean & Dedupe| S3
    B1 --> |Latest Prices| S4
    B4 --> |Latest Costs| S4
    B6 --> |Anime Linking| S4
    B1 --> |Sales Movements| S5
    B4 --> |Delivery Movements| S5
    B6 --> |SCD2 Processing| S6
    B5 --> |Aggregate by Screening| S7

    %% ETL Process 2: Silver to Gold
    S1 --> |Join with Inventory| G1
    S5 --> |Inventory Levels| G1
    S2 --> |Join with Concessions| G2
    S7 --> |Concession Revenue| G2
    S1 --> |Customer Activity| G3
    S2 --> |Customer Activity| G3
    S3 --> |Master Data| G3
    S4 --> |Product Info| G4
    S6 --> |Anime Dimensions| G5
    G3 --> |Segment Analysis| G6
    S1 --> |Performance Metrics| G7
    S2 --> |Performance Metrics| G7
    S6 --> |Anime Info| G7
    S5 --> |Stock Levels| G8
    G5 --> |Historical Sales| G9
    G1 --> |Sales History| G9
    G2 --> |Screening History| G9


    
    %% Styling
    classDef bronze fill:#8B4513,stroke:#654321,stroke-width:2px,color:#fff
    classDef silver fill:#C0C0C0,stroke:#808080,stroke-width:2px,color:#000
    classDef gold fill:#FFD700,stroke:#FFA500,stroke-width:2px,color:#000
    classDef infrastructure fill:#4169E1,stroke:#000080,stroke-width:2px,color:#fff
    classDef source fill:#228B22,stroke:#006400,stroke-width:2px,color:#fff

    class B1,B2,B3,B4,B5,B6 bronze
    class S1,S2,S3,S4,S5,S6,S7 silver
    class G1,G2,G3,G4,G5,G6,G7,G8,G9 gold
