# Marketplace Business Events Specification

> [!NOTE]
> This document specifies the 6 core business events for the **Marketplace Sandbox** real-time data platform.
> It defines the functional context, semantic meaning, business domain triggers, attribute metadata tables, validation rules, and canonical JSON payload samples for each event.
> This specification acts as the authoritative reference for Phase 2 Schema Design and Kafka Schema Registry contract definitions.

---

## 1. Governance & Architecture Overview

### 1.1 Messaging Infrastructure & Topic Conventions
All events are published to dedicated Apache Kafka topics using domain-driven naming conventions: `marketplace.<domain_subsystem>`.

| Event Identifier | Kafka Topic Name | Business Subsystem | Primary Partitioning Key |
| :--- | :--- | :--- | :--- |
| `order_creation` | `marketplace.orders` | Checkout / Order Service | `order_id` |
| `payment_updates` | `marketplace.payments` | Payment Gateway Service | `payment_id` |
| `cart_events` | `marketplace.cart` | Shopping Cart Service | `cart_id` |
| `stock_updates` | `marketplace.inventory` | Inventory Service | `product_id` |
| `user_navigation` | `marketplace.navigation` | Telemetry / Clickstream Service | `session_id` |
| `catalog_changes` | `marketplace.catalog` | Merchant / Catalog Service | `product_id` |

### 1.2 Event Standard Specifications
1. **Timestamp Standard**: All timestamps must be formatted as UTC ISO 8601 strings with millisecond precision (`YYYY-MM-DDTHH:MM:SS.sssZ`).
2. **Identifier Standard**: All unique keys (`event_id`, `order_id`, `customer_id`, etc.) use 128-bit UUID v4 format.
3. **Monetary Values**: All currency figures are represented as high-precision decimals/floats with explicit 3-letter ISO-4217 currency codes (e.g., `BRL`, `USD`).
4. **Partition Guarantee**: Messages are partitioned by primary entity keys (e.g., `order_id`, `product_id`, `session_id`) to ensure in-order processing per entity across Kafka consumer groups.

---

## 2. Comprehensive Event Specifications

---

### 2.1 Order Creation Event (`order_creation`)

* **Kafka Topic Name**: `marketplace.orders`
* **Business Domain**: Checkout & Order Management Subsystem
* **Trigger Conditions**: Emitted immediately when a customer completes the checkout flow and places a new purchase order.
* **Semantic Meaning**: Represents the formal inception of a commercial contract between customer and seller. Requires downstream inventory reservation, payment processing, and fulfillment workflow initiation.
* **Partition Key**: `order_id`

#### Attribute Table

| Field Name | Type | Description | Required / Nullable | Validation Rules & Constraints | Example Value |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `event_id` | String | Unique telemetry event identifier | Required | Valid UUID v4 | `"550e8400-e29b-41d4-a716-446655440000"` |
| `event_type` | String | Event action discriminator | Required | Must be `"ORDER_CREATED"` | `"ORDER_CREATED"` |
| `timestamp` | String | Event emission timestamp in UTC | Required | ISO 8601 string (`YYYY-MM-DDTHH:MM:SS.sssZ`) | `"2026-07-26T17:25:00.000Z"` |
| `order_id` | String | Unique identifier of the created order | Required | Valid UUID v4 | `"ord-88392-4920"` |
| `customer_id` | String | Unique identifier of the purchasing customer | Required | Valid UUID v4 | `"cust-10293"` |
| `seller_id` | String | Primary seller identifier fulfilling the order | Required | Valid UUID v4 | `"sell-99201"` |
| `order_status` | String | Initial lifecycle status of order | Required | Allowed: `"PENDING_PAYMENT"`, `"CREATED"` | `"PENDING_PAYMENT"` |
| `currency` | String | ISO 4217 currency code | Required | Exactly 3 uppercase letters | `"BRL"` |
| `subtotal_amount` | Double | Sum of line items price before adjustments | Required | Must be `> 0.00` | `250.00` |
| `discount_amount` | Double | Total monetary discount applied | Required | Must be `>= 0.00` | `10.00` |
| `shipping_fee` | Double | Total shipping / freight charge | Required | Must be `>= 0.00` | `15.00` |
| `total_amount` | Double | Final payable total (`subtotal - discount + shipping`) | Required | Must equal math calculation, `> 0.00` | `255.00` |
| `payment_method` | String | Selected payment instrument | Required | Allowed: `"CREDIT_CARD"`, `"PIX"`, `"BOLETO"`, `"DEBIT_CARD"` | `"PIX"` |
| `shipping_address` | Object | Nested delivery location metadata | Required | Non-null object conforming to address schema | `{...}` |
| `shipping_address.street` | String | Street name or thoroughfare | Required | Non-empty string | `"Av. Paulista"` |
| `shipping_address.number` | String | Building or house number | Required | Non-empty string | `"1000"` |
| `shipping_address.complement` | String | Additional building/apartment details | Nullable | Optional string | `"Apt 42"` |
| `shipping_address.city` | String | Destination city name | Required | Non-empty string | `"São Paulo"` |
| `shipping_address.state` | String | Destination state / province code | Required | Exactly 2 uppercase letters | `"SP"` |
| `shipping_address.zip_code` | String | Postal or Zip code | Required | Format regex: `^\d{5}-\d{3}$` or international zip | `"01310-100"` |
| `shipping_address.country` | String | 2-letter ISO country code | Required | Exactly 2 uppercase letters | `"BR"` |
| `items` | Array[Object] | List of purchased line items | Required | Must contain at least 1 item | `[...]` |
| `items[].item_id` | String | Unique line item identifier | Required | Valid UUID v4 | `"item-001"` |
| `items[].product_id` | String | Unique catalog product identifier | Required | Valid UUID v4 | `"prod-77123"` |
| `items[].product_name` | String | Name of product at purchase time | Required | Non-empty string | `"Wireless Headphones"` |
| `items[].quantity` | Integer | Number of units purchased | Required | Integer `>= 1` | `2` |
| `items[].unit_price` | Double | Unit price at purchase time | Required | Must be `> 0.00` | `125.00` |
| `items[].subtotal` | Double | Line total (`unit_price * quantity`) | Required | Must equal `unit_price * quantity` | `250.00` |

#### Canonical JSON Payload Sample

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "event_type": "ORDER_CREATED",
  "timestamp": "2026-07-26T17:25:00.000Z",
  "order_id": "ord-88392-4920",
  "customer_id": "cust-10293",
  "seller_id": "sell-99201",
  "order_status": "PENDING_PAYMENT",
  "currency": "BRL",
  "subtotal_amount": 250.00,
  "discount_amount": 10.00,
  "shipping_fee": 15.00,
  "total_amount": 255.00,
  "payment_method": "PIX",
  "shipping_address": {
    "street": "Av. Paulista",
    "number": "1000",
    "complement": "Apt 42",
    "city": "São Paulo",
    "state": "SP",
    "zip_code": "01310-100",
    "country": "BR"
  },
  "items": [
    {
      "item_id": "item-001",
      "product_id": "prod-77123",
      "product_name": "Wireless Headphones",
      "quantity": 2,
      "unit_price": 125.00,
      "subtotal": 250.00
    }
  ]
}
```

---

### 2.2 Payment Updates Event (`payment_updates`)

* **Kafka Topic Name**: `marketplace.payments`
* **Business Domain**: Payments & Financial Gateway Subsystem
* **Trigger Conditions**: Published whenever a payment gateway or internal financial processor updates the transaction status of an order.
* **Semantic Meaning**: Drives order processing downstream actions. An `APPROVED` status advances order fulfillment, whereas `DECLINED` or `FAILED` triggers customer alerts and releases reserved stock.
* **Partition Key**: `payment_id`

#### Attribute Table

| Field Name | Type | Description | Required / Nullable | Validation Rules & Constraints | Example Value |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `event_id` | String | Unique telemetry event identifier | Required | Valid UUID v4 | `"e01104a3-2391-4e7a-8f81-550186981290"` |
| `event_type` | String | Event action discriminator | Required | Must be `"PAYMENT_STATUS_UPDATED"` | `"PAYMENT_STATUS_UPDATED"` |
| `timestamp` | String | Event emission timestamp in UTC | Required | ISO 8601 string (`YYYY-MM-DDTHH:MM:SS.sssZ`) | `"2026-07-26T17:25:05.120Z"` |
| `payment_id` | String | Unique transaction identifier | Required | Valid UUID v4 | `"pay-44910-8812"` |
| `order_id` | String | Associated order identifier | Required | Valid UUID v4 | `"ord-88392-4920"` |
| `customer_id` | String | Unique customer identifier | Required | Valid UUID v4 | `"cust-10293"` |
| `payment_method` | String | Payment instrument evaluated | Required | Allowed: `"CREDIT_CARD"`, `"PIX"`, `"BOLETO"`, `"DEBIT_CARD"` | `"PIX"` |
| `payment_status` | String | Current monetary execution status | Required | Allowed: `"AUTHORIZED"`, `"APPROVED"`, `"DECLINED"`, `"REFUNDED"`, `"FAILED"` | `"APPROVED"` |
| `amount` | Double | Total monetary amount processed | Required | Must be `> 0.00` | `255.00` |
| `currency` | String | ISO 4217 currency code | Required | Exactly 3 uppercase letters | `"BRL"` |
| `gateway_provider` | String | Payment gateway provider identifier | Required | Allowed: `"STRIPE"`, `"PAGSEGURO"`, `"MOCK_GATEWAY"` | `"MOCK_GATEWAY"` |
| `transaction_reference` | String | External gateway authorization reference | Nullable | String or null | `"tx_pix_991823712"` |
| `failure_reason` | String | Diagnostic error code or rejection detail | Nullable | Non-null if status is `"DECLINED"` or `"FAILED"` | `null` |
| `installments` | Integer | Number of payment installments | Required | Integer `>= 1` | `1` |

#### Canonical JSON Payload Sample

```json
{
  "event_id": "e01104a3-2391-4e7a-8f81-550186981290",
  "event_type": "PAYMENT_STATUS_UPDATED",
  "timestamp": "2026-07-26T17:25:05.120Z",
  "payment_id": "pay-44910-8812",
  "order_id": "ord-88392-4920",
  "customer_id": "cust-10293",
  "payment_method": "PIX",
  "payment_status": "APPROVED",
  "amount": 255.00,
  "currency": "BRL",
  "gateway_provider": "MOCK_GATEWAY",
  "transaction_reference": "tx_pix_991823712",
  "failure_reason": null,
  "installments": 1
}
```

---

### 2.3 Cart Events (`cart_events`)

* **Kafka Topic Name**: `marketplace.cart`
* **Business Domain**: Shopping Cart & Pre-Purchase Telemetry Subsystem
* **Trigger Conditions**: Published whenever a user modifies items in their active digital shopping cart (adding, removing, altering quantity, or flushing cart).
* **Semantic Meaning**: Captures real-time purchasing intent. Crucial for cart abandonment detection, recommendation engine features, and real-time conversion funnels.
* **Partition Key**: `cart_id`

#### Attribute Table

| Field Name | Type | Description | Required / Nullable | Validation Rules & Constraints | Example Value |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `event_id` | String | Unique telemetry event identifier | Required | Valid UUID v4 | `"91823ab1-1293-4819-aa29-881273619283"` |
| `event_type` | String | Cart operation discriminator | Required | Allowed: `"CART_ITEM_ADDED"`, `"CART_ITEM_REMOVED"`, `"CART_QUANTITY_UPDATED"`, `"CART_CLEARED"` | `"CART_ITEM_ADDED"` |
| `timestamp` | String | Event emission timestamp in UTC | Required | ISO 8601 string (`YYYY-MM-DDTHH:MM:SS.sssZ`) | `"2026-07-26T17:20:12.450Z"` |
| `cart_id` | String | Unique active cart identifier | Required | Valid UUID v4 | `"cart-991283"` |
| `user_id` | String | Authenticated user identifier | Nullable | Valid UUID v4 if logged in, otherwise null | `"cust-10293"` |
| `session_id` | String | Web/App session tracking identifier | Required | Valid UUID v4 | `"sess-88192-3312"` |
| `product_id` | String | Catalog product target of change | Nullable | Valid UUID v4; null only when `"CART_CLEARED"` | `"prod-77123"` |
| `quantity` | Integer | Item quantity added, removed, or set | Nullable | Integer `>= 1`; null only when `"CART_CLEARED"` | `2` |
| `unit_price` | Double | Listed unit price at action time | Nullable | Must be `> 0.00`; null only when `"CART_CLEARED"` | `125.00` |
| `cart_total_items` | Integer | Total count of items in cart post-event | Required | Integer `>= 0` | `3` |
| `cart_total_value` | Double | Accumulated total value of cart post-event | Required | Double `>= 0.00` | `375.00` |

#### Canonical JSON Payload Sample

```json
{
  "event_id": "91823ab1-1293-4819-aa29-881273619283",
  "event_type": "CART_ITEM_ADDED",
  "timestamp": "2026-07-26T17:20:12.450Z",
  "cart_id": "cart-991283",
  "user_id": "cust-10293",
  "session_id": "sess-88192-3312",
  "product_id": "prod-77123",
  "quantity": 2,
  "unit_price": 125.00,
  "cart_total_items": 3,
  "cart_total_value": 375.00
}
```

---

### 2.4 Stock Updates Event (`stock_updates`)

* **Kafka Topic Name**: `marketplace.inventory`
* **Business Domain**: Inventory & Warehousing Subsystem
* **Trigger Conditions**: Emitted by inventory systems when stock counts shift due to order reservations, order cancellations, stock intake, or audit adjustments.
* **Semantic Meaning**: Maintains exact real-time visibility over inventory availability across distribution centers, preventing overselling and triggering auto-replenishment.
* **Partition Key**: `product_id`

#### Attribute Table

| Field Name | Type | Description | Required / Nullable | Validation Rules & Constraints | Example Value |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `event_id` | String | Unique telemetry event identifier | Required | Valid UUID v4 | `"a1b2c3d4-e5f6-7890-abcd-ef1234567890"` |
| `event_type` | String | Inventory operation discriminator | Required | Allowed: `"STOCK_RESERVED"`, `"STOCK_RELEASED"`, `"STOCK_REPLENISHED"`, `"STOCK_ADJUSTED"` | `"STOCK_RESERVED"` |
| `timestamp` | String | Event emission timestamp in UTC | Required | ISO 8601 string (`YYYY-MM-DDTHH:MM:SS.sssZ`) | `"2026-07-26T17:25:01.000Z"` |
| `inventory_id` | String | Inventory audit log identifier | Required | Valid UUID v4 | `"inv-55102"` |
| `product_id` | String | Target catalog product SKU/ID | Required | Valid UUID v4 | `"prod-77123"` |
| `seller_id` | String | Merchant owning the inventory | Required | Valid UUID v4 | `"sell-99201"` |
| `warehouse_id` | String | Fulfillment center / warehouse ID | Required | Valid UUID v4 or standard code | `"wh-sp-01"` |
| `quantity_change` | Integer | Quantity variation delta | Required | Negative for reservations, positive for additions | `-2` |
| `previous_stock` | Integer | Available stock prior to update | Required | Integer `>= 0` | `50` |
| `new_stock` | Integer | Available stock post-update | Required | Integer `>= 0`, must equal `previous_stock + quantity_change` | `48` |
| `reason` | String | Business rationale for change | Required | Allowed: `"ORDER_RESERVATION"`, `"CANCELLED_ORDER"`, `"REPLENISHMENT"`, `"DAMAGED_ITEM"` | `"ORDER_RESERVATION"` |
| `order_id` | String | Associated order identifier | Nullable | Valid UUID v4 if caused by an order, else null | `"ord-88392-4920"` |

#### Canonical JSON Payload Sample

```json
{
  "event_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "event_type": "STOCK_RESERVED",
  "timestamp": "2026-07-26T17:25:01.000Z",
  "inventory_id": "inv-55102",
  "product_id": "prod-77123",
  "seller_id": "sell-99201",
  "warehouse_id": "wh-sp-01",
  "quantity_change": -2,
  "previous_stock": 50,
  "new_stock": 48,
  "reason": "ORDER_RESERVATION",
  "order_id": "ord-88392-4920"
}
```

---

### 2.5 User Navigation Event (`user_navigation`)

* **Kafka Topic Name**: `marketplace.navigation`
* **Business Domain**: Clickstream Telemetry & User Behavior Subsystem
* **Trigger Conditions**: Emitted continuously by web frontends and mobile clients as users navigate pages, search products, or view categories.
* **Semantic Meaning**: Provides high-volume clickstream events for computing CTR, bounce rates, conversion funnels, session duration, and real-time user profiling.
* **Partition Key**: `session_id`

#### Attribute Table

| Field Name | Type | Description | Required / Nullable | Validation Rules & Constraints | Example Value |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `event_id` | String | Unique telemetry event identifier | Required | Valid UUID v4 | `"77128441-2990-11ec-abcd-001122334455"` |
| `event_type` | String | Interaction type discriminator | Required | Allowed: `"PAGE_VIEW"`, `"PRODUCT_CLICK"`, `"SEARCH_QUERY"`, `"CATEGORY_BROWSE"` | `"PRODUCT_CLICK"` |
| `timestamp` | String | Event emission timestamp in UTC | Required | ISO 8601 string (`YYYY-MM-DDTHH:MM:SS.sssZ`) | `"2026-07-26T17:18:40.000Z"` |
| `session_id` | String | User active session identifier | Required | Valid UUID v4 | `"sess-88192-3312"` |
| `user_id` | String | Authenticated user identifier | Nullable | Valid UUID v4 if logged in, otherwise null | `"cust-10293"` |
| `device_type` | String | Client hardware classification | Required | Allowed: `"DESKTOP"`, `"MOBILE"`, `"TABLET"` | `"MOBILE"` |
| `ip_address` | String | Client IP address for geolocation | Required | Valid IPv4 or IPv6 string | `"189.100.22.15"` |
| `user_agent` | String | HTTP User-Agent string | Required | Non-empty string | `"Mozilla/5.0 (iPhone; CPU OS 17_5...)"` |
| `page_url` | String | Fully qualified target URL | Required | Valid URL format | `"https://marketplace.com/products/prod-77123"` |
| `referrer_url` | String | HTTP Referrer / Previous page URL | Nullable | Valid URL format or null | `"https://marketplace.com/categories/electronics"` |
| `product_id` | String | Viewed/clicked product identifier | Nullable | Valid UUID v4 if product event | `"prod-77123"` |
| `category_id` | String | Viewed category identifier | Nullable | Valid UUID v4 if category event | `"cat-elec-01"` |
| `search_term` | String | Query term entered in search bar | Nullable | Non-null only when `event_type` is `"SEARCH_QUERY"` | `null` |

#### Canonical JSON Payload Sample

```json
{
  "event_id": "77128441-2990-11ec-abcd-001122334455",
  "event_type": "PRODUCT_CLICK",
  "timestamp": "2026-07-26T17:18:40.000Z",
  "session_id": "sess-88192-3312",
  "user_id": "cust-10293",
  "device_type": "MOBILE",
  "ip_address": "189.100.22.15",
  "user_agent": "Mozilla/5.0 (iPhone; CPU OS 17_5 like Mac OS X)",
  "page_url": "https://marketplace.com/products/prod-77123",
  "referrer_url": "https://marketplace.com/categories/electronics",
  "product_id": "prod-77123",
  "category_id": "cat-elec-01",
  "search_term": null
}
```

---

### 2.6 Catalog Changes Event (`catalog_changes`)

* **Kafka Topic Name**: `marketplace.catalog`
* **Business Domain**: Product Catalog & Merchant Administration Subsystem
* **Trigger Conditions**: Emitted when a seller or catalog admin creates a new product, updates details, changes price, or deactivates a listing.
* **Semantic Meaning**: Signals downstream search indexers, product cache managers, and price tracking engines to re-index or adjust product availability.
* **Partition Key**: `product_id`

#### Attribute Table

| Field Name | Type | Description | Required / Nullable | Validation Rules & Constraints | Example Value |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `event_id` | String | Unique telemetry event identifier | Required | Valid UUID v4 | `"c9981234-4412-9901-0012-aabbccddeeff"` |
| `event_type` | String | Catalog action discriminator | Required | Allowed: `"PRODUCT_CREATED"`, `"PRODUCT_UPDATED"`, `"PRICE_CHANGED"`, `"PRODUCT_DEACTIVATED"` | `"PRICE_CHANGED"` |
| `timestamp` | String | Event emission timestamp in UTC | Required | ISO 8601 string (`YYYY-MM-DDTHH:MM:SS.sssZ`) | `"2026-07-26T16:00:00.000Z"` |
| `product_id` | String | Unique catalog product identifier | Required | Valid UUID v4 | `"prod-77123"` |
| `seller_id` | String | Merchant owning the product | Required | Valid UUID v4 | `"sell-99201"` |
| `sku` | String | Merchant Stock Keeping Unit code | Required | Non-empty alphanumeric string | `"AUDIO-HD-WIFI-01"` |
| `product_name` | String | Commercial title of the product | Required | Non-empty string | `"Wireless Headphones Noise Cancelling"` |
| `category_id` | String | Primary category identifier | Required | Valid UUID v4 | `"cat-elec-01"` |
| `brand` | String | Brand or manufacturer name | Nullable | Optional string | `"AudioTech"` |
| `price` | Double | New list price in standard currency | Required | Must be `> 0.00` | `125.00` |
| `previous_price` | Double | Prior list price before modification | Nullable | Must be `> 0.00`; null if `"PRODUCT_CREATED"` | `149.99` |
| `is_active` | Boolean | Catalog availability flag | Required | `true` or `false` | `true` |
| `updated_by` | String | User or system agent making edit | Required | Non-empty user identifier | `"user-admin-04"` |

#### Canonical JSON Payload Sample

```json
{
  "event_id": "c9981234-4412-9901-0012-aabbccddeeff",
  "event_type": "PRICE_CHANGED",
  "timestamp": "2026-07-26T16:00:00.000Z",
  "product_id": "prod-77123",
  "seller_id": "sell-99201",
  "sku": "AUDIO-HD-WIFI-01",
  "product_name": "Wireless Headphones Noise Cancelling",
  "category_id": "cat-elec-01",
  "brand": "AudioTech",
  "price": 125.00,
  "previous_price": 149.99,
  "is_active": true,
  "updated_by": "user-admin-04"
}
```

---

## 3. Cross-Event Comparison Matrix

| Event Name | Topic Name | Partition Key | Throughput Profile | Main Downstream Consumers |
| :--- | :--- | :--- | :--- | :--- |
| `order_creation` | `marketplace.orders` | `order_id` | Medium Volume / High Priority | Ingestion Pipeline, Inventory, Payment, Medallion Bronze |
| `payment_updates` | `marketplace.payments` | `payment_id` | Medium Volume / High Priority | Ingestion Pipeline, Order Status Worker, Medallion Bronze |
| `cart_events` | `marketplace.cart` | `cart_id` | High Volume / Medium Priority | Ingestion Pipeline, Cart Abandonment Analytics, Medallion Bronze |
| `stock_updates` | `marketplace.inventory` | `product_id` | High Volume / High Priority | Ingestion Pipeline, Catalog Availability, Medallion Bronze |
| `user_navigation` | `marketplace.navigation` | `session_id` | Ultra-High Volume / Low Latency | Ingestion Pipeline, Clickstream Analytics, Medallion Bronze |
| `catalog_changes` | `marketplace.catalog` | `product_id` | Low Volume / Medium Priority | Search Engine Indexer, Redis Cache, Medallion Bronze |

---

## 4. Next Steps & References

- Refer to active phase tasks in [docs/context/current-step.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/context/current-step.md).
- These 6 event definitions will be translated into formal Avro and JSON Schema specifications in Phase 2 (`data-engineer-agent`) and programmatically registered into the Confluent Schema Registry in Phase 3 (`software-engineer-agent`).
