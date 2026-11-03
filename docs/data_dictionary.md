# Data Dictionary
## Real-Time E-Commerce Data Engineering and Analytics Platform

### 1. Project Overview

This project uses the Brazilian E-Commerce Public Dataset by Olist to build a real-time data engineering and analytics platform using Python, Apache Kafka, Apache Spark, and Google BigQuery.

The source data is organized into relational tables. Python transforms order-item records into JSON events, which are published to Kafka for downstream stream processing.

### 2. Database Tables

#### 2.1 Customers Table

**Purpose:** Stores customer information.

| Column | Data Type | Key | Description |
|---|---|---|---|
| customer_id | STRING | Primary Key | Unique customer identifier for an order |
| customer_unique_id | STRING | | Unique customer identifier across orders |
| customer_city | STRING | | Customer's city |
| customer_state | STRING | | Customer's state |

#### 2.2 Orders Table

**Purpose:** Stores information about customer orders.

| Column | Data Type | Key | Description |
|---|---|---|---|
| order_id | STRING | Primary Key | Unique order identifier |
| customer_id | STRING | Foreign Key | References customers.customer_id |
| order_status | STRING | | Current order status |
| order_purchase_timestamp | DATETIME | | Date and time when the order was placed |

#### 2.3 Order Items Table

**Purpose:** Stores individual products included in each order.

| Column | Data Type | Key | Description |
|---|---|---|---|
| order_id | STRING | Primary Key, Foreign Key | References orders.order_id |
| order_item_id | INTEGER | Primary Key | Item sequence number within an order |
| product_id | STRING | Foreign Key | References products.product_id |
| seller_id | STRING | Foreign Key | References sellers.seller_id |
| price | FLOAT | | Price of the item |
| freight_value | FLOAT | | Shipping cost for the item |

The composite primary key is (order_id, order_item_id).

#### 2.4 Products Table

**Purpose:** Stores product details.

| Column | Data Type | Key | Description |
|---|---|---|---|
| product_id | STRING | Primary Key | Unique product identifier |
| product_category_name | STRING | | Product category |

#### 2.5 Sellers Table

**Purpose:** Stores seller information.

| Column | Data Type | Key | Description |
|---|---|---|---|
| seller_id | STRING | Primary Key | Unique seller identifier |
| seller_city | STRING | | Seller's city |
| seller_state | STRING | | Seller's state |

#### 2.6 Payments Table

**Purpose:** Stores payment details associated with orders.

| Column | Data Type | Key | Description |
|---|---|---|---|
| order_id | STRING | Primary Key, Foreign Key | References orders.order_id |
| payment_sequential | INTEGER | Primary Key | Payment sequence number |
| payment_type | STRING | | Payment method |
| payment_value | FLOAT | | Payment amount |

The composite primary key is (order_id, payment_sequential).

#### 2.7 Reviews Table

**Purpose:** Stores customer reviews associated with orders.

| Column | Data Type | Key | Description |
|---|---|---|---|
| review_id | STRING | Primary Key | Unique review identifier |
| order_id | STRING | Foreign Key | References orders.order_id |
| review_score | INTEGER | | Customer review score |

### 3. Entity Relationships

- One customer can place multiple orders.
- One order can contain multiple order items.
- One product can appear in multiple order items.
- One seller can fulfill multiple order items.
- One order can have multiple payment records.
- One order can have multiple review records.

### 4. Kafka Event Schema

The ingestion module converts each order item into one JSON event.

| Field | Data Type | Description |
|---|---|---|
| event_id | STRING | Unique event identifier |
| order_id | STRING | Order identifier |
| order_item_id | INTEGER | Item sequence within the order |
| customer_id | STRING | Customer identifier |
| product_id | STRING | Product identifier |
| seller_id | STRING | Seller identifier |
| order_status | STRING | Order status |
| order_timestamp | STRING | Order purchase timestamp in ISO format |
| category | STRING | Product category |
| price | FLOAT | Product price |
| freight_value | FLOAT | Shipping cost |
| item_total | FLOAT | Price plus freight value |

### 5. Event Generation Rules

1. Each order item generates one event.
2. The event ID is created by combining order_id and order_item_id.
3. The event timestamp is taken from the order purchase timestamp.
4. Missing product categories are represented as "unknown".
5. Negative price and freight values are rejected.
6. item_total is calculated as:

   item_total = price + freight_value

**Note:** item_total represents the item price plus its shipping cost. It is not necessarily the total value of the complete order.

### 6. Data Flow

1. Olist CSV files are loaded using Python and Pandas.
2. Orders, order items, and products are joined to create order-item events.
3. Events are converted into JSON format.
4. Python Kafka Producer publishes the events to the ecommerce_orders topic.
5. Apache Spark consumes and processes the events.
6. Processed data is stored in Parquet format and loaded into BigQuery for analytics.