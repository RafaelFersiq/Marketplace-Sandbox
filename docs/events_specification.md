# Marketplace Business Events Specification

> [!NOTE]
> This document specifies the 6 core business events for the **Marketplace Sandbox** real-time data platform.
> It defines the functional context, semantic meaning, business domain triggers, schema metadata links, attribute tables, validation rules, and canonical JSON payload samples for each event.
> This specification acts as the authoritative reference for Schema Design, Avro logical type encoding, and Confluent Schema Registry integration.

---

## 1. Governance & Architecture Overview

### 1.1 Messaging Infrastructure & Topic Conventions
All events are published to dedicated Apache Kafka topics using domain-driven naming conventions (`marketplace.<domain_subsystem>`) and registered in Confluent Schema Registry under value subjects (`marketplace.<domain_subsystem>-value`).

| Event Identifier | Kafka Topic Name | Schema Subject Name | Primary Partition Key | Business Subsystem |
| :--- | :--- | :--- | :--- | :--- |
| `order_creation` | `marketplace.orders` | `marketplace.orders-value` | `order_id` | Checkout / Order Service |
| `payment_updates` | `marketplace.payments` | `marketplace.payments-value` | `payment_id` | Payment Gateway Service |
| `cart_events` | `marketplace.cart` | `marketplace.cart-value` | `cart_id` | Shopping Cart Service |
| `stock_updates` | `marketplace.inventory` | `marketplace.inventory-value` | `product_id` | Inventory Service |
| `user_navigation` | `marketplace.navigation` | `marketplace.navigation-value` | `session_id` | Telemetry / Clickstream Service |
| `catalog_changes` | `marketplace.catalog` | `marketplace.catalog-value` | `product_id` | Merchant / Catalog Service |

### 1.2 Event Standard Specifications
1. **Timestamp Standard**: All event timestamps use Avro `timestamp-millis` logical type (64-bit signed integer representing milliseconds since UTC Unix epoch, e.g. `1785000300000`).
2. **Identifier Standard**: All unique entity keys (`event_id`, `order_id`, `customer_id`, etc.) use 128-bit UUID v4 format.
3. **Monetary Values**: All currency figures are represented as Avro `decimal` logical type with fixed precision 18 and scale 4 (`decimal(18,4)`), accompanied by explicit 3-letter ISO-4217 currency codes (e.g., `"BRL"`, `"USD"`).
4. **Partition Guarantee**: Messages are partitioned by primary entity keys (e.g., `order_id`, `product_id`, `session_id`) to ensure in-order processing per entity across Kafka consumer groups.

### 1.3 Schema Registry Caching & Compatibility Governance
1. **Schema Registry Integration**: Managed by [ingestion/schema_registry.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/schema_registry.py).
2. **In-Memory LRU Caching**: Schema definitions fetched by schema ID during payload deserialization are cached locally in `_schema_id_cache` (`SchemaRegistryManager.get_schema_by_id()`), eliminating redundant HTTP REST lookups.
3. **Compatibility Level Enforcement**: All registered subjects enforce `BACKWARD` compatibility mode (`set_compatibility_level("BACKWARD", subject)`), guaranteeing that updated consumer pipelines remain fully compatible with historical messages.
4. **Binary Wire Format**: Messages conform to the Confluent 5-byte header specification (`0x00` magic byte + 4-byte big-endian `schema_id` + schemaless Avro payload).
5. **Python SerDe Integration**: Interfaced via [ingestion/serialization.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/serialization.py) (`AvroSerializerDeserializer`), automatically converting Python `datetime` objects and `decimal.Decimal` instances during Avro encoding and decoding.

---

## 2. Comprehensive Event Specifications

---

### 2.1 Order Creation Event (`order_creation`)

* **Kafka Topic Name**: `marketplace.orders`
* **Schema Subject**: `marketplace.orders-value`
* **Avro Schema File**: [order_creation.avsc](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/schemas/avro/order_creation.avsc)
* **Business Domain**: Checkout & Order Management Subsystem
* **Trigger Conditions**: Emitted immediately when a customer completes the checkout flow and places a new purchase order.
* **Semantic Meaning**: Represents the formal inception of a commercial contract between customer and seller. Requires downstream inventory reservation, payment processing, and fulfillment workflow initiation.
* **Partition Key**: `order_id`

#### Attribute Table

| Field Name | Type | Description | Required / Nullable | Validation Rules & Constraints | Example Value |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `event_id` | String | Unique telemetry event identifier | Required | Valid UUID v4 | `"550e8400-e29b-41d4-a716-446655440000"` |
| `event_type` | String | Event action discriminator | Required | Must be `"ORDER_CREATED"` | `"ORDER_CREATED"` |
| `timestamp` | Long (timestamp-millis) | Milliseconds since UTC Unix epoch | Required | 64-bit integer >= 0 | `1785000300000` |
| `order_id` | String | Unique identifier of the created order | Required | Valid UUID v4 | `"ord-88392-4920"` |
| `customer_id` | String | Unique identifier of the purchasing customer | Required | Valid UUID v4 | `"cust-10293"` |
| `seller_id` | String | Primary seller identifier fulfilling the order | Required | Valid UUID v4 | `"sell-99201"` |
| `order_status` | String | Initial lifecycle status of order | Required | Allowed: `"PENDING_PAYMENT"`, `"CREATED"` | `"PENDING_PAYMENT"` |
| `currency` | String | ISO 4217 currency code | Required | Exactly 3 uppercase letters | `"BRL"` |
| `subtotal_amount` | Decimal (18,4) | Monetary value represented as fixed-precision decimal (precision 18, scale 4) | Required | Must be `> 0.0000` | `250.0000` |
| `discount_amount` | Decimal (18,4) | Monetary value represented as fixed-precision decimal (precision 18, scale 4) | Required | Must be `>= 0.0000` | `10.0000` |
| `shipping_fee` | Decimal (18,4) | Monetary value represented as fixed-precision decimal (precision 18, scale 4) | Required | Must be `>= 0.0000` | `15.0000` |
| `total_amount` | Decimal (18,4) | Monetary value represented as fixed-precision decimal (precision 18, scale 4) | Required | Must equal math calculation, `> 0.0000` | `255.0000` |
| `payment_method` | String | Selected payment instrument | Required | Allowed: `"CREDIT_CARD"`, `"PIX"`, `"BOLETO"`, `"DEBIT_CARD"` | `"PIX"` |
| `shipping_address` | Record | Nested delivery location metadata | Required | Non-null object conforming to address schema | `{...}` |
| `shipping_address.street` | String | Street name or thoroughfare | Required | Non-empty string | `"Av. Paulista"` |
| `shipping_address.number` | String | Building or house number | Required | Non-empty string | `"1000"` |
| `shipping_address.complement` | String / Null | Additional building/apartment details | Nullable | Optional string or null | `"Apt 42"` |
| `shipping_address.city` | String | Destination city name | Required | Non-empty string | `"São Paulo"` |
| `shipping_address.state` | String | Destination state / province code | Required | Exactly 2 uppercase letters | `"SP"` |
| `shipping_address.zip_code` | String | Postal or Zip code | Required | Format regex: `^\d{5}-\d{3}$` or international zip | `"01310-100"` |
| `shipping_address.country` | String | 2-letter ISO country code | Required | Exactly 2 uppercase letters | `"BR"` |
| `items` | Array[Record] | List of purchased line items | Required | Must contain at least 1 item | `[...]` |
| `items[].item_id` | String | Unique line item identifier | Required | Valid UUID v4 | `"item-001"` |
| `items[].product_id` | String | Unique catalog product identifier | Required | Valid UUID v4 | `"prod-77123"` |
| `items[].product_name` | String | Name of product at purchase time | Required | Non-empty string | `"Wireless Headphones"` |
| `items[].quantity` | Integer | Number of units purchased | Required | Integer `>= 1` | `2` |
| `items[].unit_price` | Decimal (18,4) | Monetary value represented as fixed-precision decimal (precision 18, scale 4) | Required | Must be `> 0.0000` | `125.0000` |
| `items[].subtotal` | Decimal (18,4) | Monetary value represented as fixed-precision decimal (precision 18, scale 4) | Required | Must equal `unit_price * quantity` | `250.0000` |

#### Canonical JSON Payload Sample

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "event_type": "ORDER_CREATED",
  "timestamp": 1785000300000,
  "order_id": "ord-88392-4920",
  "customer_id": "cust-10293",
  "seller_id": "sell-99201",
  "order_status": "PENDING_PAYMENT",
  "currency": "BRL",
  "subtotal_amount": 250.0000,
  "discount_amount": 10.0000,
  "shipping_fee": 15.0000,
  "total_amount": 255.0000,
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
      "unit_price": 125.0000,
      "subtotal": 250.0000
    }
  ]
}
```

---

### 2.2 Payment Updates Event (`payment_updates`)

* **Kafka Topic Name**: `marketplace.payments`
* **Schema Subject**: `marketplace.payments-value`
* **Avro Schema File**: [payment_updates.avsc](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/schemas/avro/payment_updates.avsc)
* **Business Domain**: Payments & Financial Gateway Subsystem
* **Trigger Conditions**: Published whenever a payment gateway or internal financial processor updates the transaction status of an order.
* **Semantic Meaning**: Drives order processing downstream actions. An `APPROVED` status advances order fulfillment, whereas `DECLINED` or `FAILED` triggers customer alerts and releases reserved stock.
* **Partition Key**: `payment_id`

#### Attribute Table

| Field Name | Type | Description | Required / Nullable | Validation Rules & Constraints | Example Value |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `event_id` | String | Unique telemetry event identifier | Required | Valid UUID v4 | `"e01104a3-2391-4e7a-8f81-550186981290"` |
| `event_type` | String | Event action discriminator | Required | Must be `"PAYMENT_STATUS_UPDATED"` | `"PAYMENT_STATUS_UPDATED"` |
| `timestamp` | Long (timestamp-millis) | Milliseconds since UTC Unix epoch | Required | 64-bit integer >= 0 | `1785000305120` |
| `payment_id` | String | Unique transaction identifier | Required | Valid UUID v4 | `"pay-44910-8812"` |
| `order_id` | String | Associated order identifier | Required | Valid UUID v4 | `"ord-88392-4920"` |
| `customer_id` | String | Unique customer identifier | Required | Valid UUID v4 | `"cust-10293"` |
| `payment_method` | String | Payment instrument evaluated | Required | Allowed: `"CREDIT_CARD"`, `"PIX"`, `"BOLETO"`, `"DEBIT_CARD"` | `"PIX"` |
| `payment_status` | String | Current monetary execution status | Required | Allowed: `"AUTHORIZED"`, `"APPROVED"`, `"DECLINED"`, `"REFUNDED"`, `"FAILED"` | `"APPROVED"` |
| `amount` | Decimal (18,4) | Monetary value represented as fixed-precision decimal (precision 18, scale 4) | Required | Must be `> 0.0000` | `255.0000` |
| `currency` | String | ISO 4217 currency code | Required | Exactly 3 uppercase letters | `"BRL"` |
| `gateway_provider` | String | Payment gateway provider identifier | Required | Allowed: `"STRIPE"`, `"PAGSEGURO"`, `"MOCK_GATEWAY"` | `"MOCK_GATEWAY"` |
| `transaction_reference` | String / Null | External gateway authorization reference | Nullable | String or null | `"tx_pix_991823712"` |
| `failure_reason` | String / Null | Diagnostic error code or rejection detail | Nullable | Non-null if status is `"DECLINED"` or `"FAILED"` | `null` |
| `installments` | Integer | Number of payment installments | Required | Integer `>= 1` | `1` |

#### Canonical JSON Payload Sample

```json
{
  "event_id": "e01104a3-2391-4e7a-8f81-550186981290",
  "event_type": "PAYMENT_STATUS_UPDATED",
  "timestamp": 1785000305120,
  "payment_id": "pay-44910-8812",
  "order_id": "ord-88392-4920",
  "customer_id": "cust-10293",
  "payment_method": "PIX",
  "payment_status": "APPROVED",
  "amount": 255.0000,
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
* **Schema Subject**: `marketplace.cart-value`
* **Avro Schema File**: [cart_events.avsc](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/schemas/avro/cart_events.avsc)
* **Business Domain**: Shopping Cart & Pre-Purchase Telemetry Subsystem
* **Trigger Conditions**: Published whenever a user modifies items in their active digital shopping cart (adding, removing, altering quantity, or flushing cart).
* **Semantic Meaning**: Captures real-time purchasing intent. Crucial for cart abandonment detection, recommendation engine features, and real-time conversion funnels.
* **Partition Key**: `cart_id`

#### Attribute Table

| Field Name | Type | Description | Required / Nullable | Validation Rules & Constraints | Example Value |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `event_id` | String | Unique telemetry event identifier | Required | Valid UUID v4 | `"91823ab1-1293-4819-aa29-881273619283"` |
| `event_type` | String | Cart operation discriminator | Required | Allowed: `"CART_ITEM_ADDED"`, `"CART_ITEM_REMOVED"`, `"CART_QUANTITY_UPDATED"`, `"CART_CLEARED"` | `"CART_ITEM_ADDED"` |
| `timestamp` | Long (timestamp-millis) | Milliseconds since UTC Unix epoch | Required | 64-bit integer >= 0 | `1785000012450` |
| `cart_id` | String | Unique active cart identifier | Required | Valid UUID v4 | `"cart-991283"` |
| `user_id` | String / Null | Authenticated user identifier | Nullable | Valid UUID v4 if logged in, otherwise null | `"cust-10293"` |
| `session_id` | String | Web/App session tracking identifier | Required | Valid UUID v4 | `"sess-88192-3312"` |
| `product_id` | String / Null | Catalog product target of change | Nullable | Valid UUID v4; null only when `"CART_CLEARED"` | `"prod-77123"` |
| `quantity` | Integer / Null | Item quantity added, removed, or set | Nullable | Integer `>= 1`; null only when `"CART_CLEARED"` | `2` |
| `unit_price` | Decimal (18,4) / Null | Monetary value represented as fixed-precision decimal (precision 18, scale 4) | Nullable | Must be `> 0.0000`; null only when `"CART_CLEARED"` | `125.0000` |
| `cart_total_items` | Integer | Total count of items in cart post-event | Required | Integer `>= 0` | `3` |
| `cart_total_value` | Decimal (18,4) | Monetary value represented as fixed-precision decimal (precision 18, scale 4) | Required | Decimal `>= 0.0000` | `375.0000` |

#### Canonical JSON Payload Sample

```json
{
  "event_id": "91823ab1-1293-4819-aa29-881273619283",
  "event_type": "CART_ITEM_ADDED",
  "timestamp": 1785000012450,
  "cart_id": "cart-991283",
  "user_id": "cust-10293",
  "session_id": "sess-88192-3312",
  "product_id": "prod-77123",
  "quantity": 2,
  "unit_price": 125.0000,
  "cart_total_items": 3,
  "cart_total_value": 375.0000
}
```

---

### 2.4 Stock Updates Event (`stock_updates`)

* **Kafka Topic Name**: `marketplace.inventory`
* **Schema Subject**: `marketplace.inventory-value`
* **Avro Schema File**: [stock_updates.avsc](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/schemas/avro/stock_updates.avsc)
* **Business Domain**: Inventory & Warehousing Subsystem
* **Trigger Conditions**: Emitted by inventory systems when stock counts shift due to order reservations, order cancellations, stock intake, or audit adjustments.
* **Semantic Meaning**: Maintains exact real-time visibility over inventory availability across distribution centers, preventing overselling and triggering auto-replenishment.
* **Partition Key**: `product_id`

#### Attribute Table

| Field Name | Type | Description | Required / Nullable | Validation Rules & Constraints | Example Value |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `event_id` | String | Unique telemetry event identifier | Required | Valid UUID v4 | `"a1b2c3d4-e5f6-7890-abcd-ef1234567890"` |
| `event_type` | String | Inventory operation discriminator | Required | Allowed: `"STOCK_RESERVED"`, `"STOCK_RELEASED"`, `"STOCK_REPLENISHED"`, `"STOCK_ADJUSTED"` | `"STOCK_RESERVED"` |
| `timestamp` | Long (timestamp-millis) | Milliseconds since UTC Unix epoch | Required | 64-bit integer >= 0 | `1785000301000` |
| `inventory_id` | String | Inventory audit log identifier | Required | Valid UUID v4 | `"inv-55102"` |
| `product_id` | String | Target catalog product SKU/ID | Required | Valid UUID v4 | `"prod-77123"` |
| `seller_id` | String | Merchant owning the inventory | Required | Valid UUID v4 | `"sell-99201"` |
| `warehouse_id` | String | Fulfillment center / warehouse ID | Required | Valid UUID v4 or standard code | `"wh-sp-01"` |
| `quantity_change` | Integer | Quantity variation delta | Required | Negative for reservations, positive for additions | `-2` |
| `previous_stock` | Integer | Available stock prior to update | Required | Integer `>= 0` | `50` |
| `new_stock` | Integer | Available stock post-update | Required | Integer `>= 0`, must equal `previous_stock + quantity_change` | `48` |
| `reason` | String | Business rationale for change | Required | Allowed: `"ORDER_RESERVATION"`, `"CANCELLED_ORDER"`, `"REPLENISHMENT"`, `"DAMAGED_ITEM"` | `"ORDER_RESERVATION"` |
| `order_id` | String / Null | Associated order identifier | Nullable | Valid UUID v4 if caused by an order, else null | `"ord-88392-4920"` |

#### Canonical JSON Payload Sample

```json
{
  "event_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "event_type": "STOCK_RESERVED",
  "timestamp": 1785000301000,
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
* **Schema Subject**: `marketplace.navigation-value`
* **Avro Schema File**: [user_navigation.avsc](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/schemas/avro/user_navigation.avsc)
* **Business Domain**: Clickstream Telemetry & User Behavior Subsystem
* **Trigger Conditions**: Emitted continuously by web frontends and mobile clients as users navigate pages, search products, or view categories.
* **Semantic Meaning**: Provides high-volume clickstream events for computing CTR, bounce rates, conversion funnels, session duration, and real-time user profiling.
* **Partition Key**: `session_id`

#### Attribute Table

| Field Name | Type | Description | Required / Nullable | Validation Rules & Constraints | Example Value |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `event_id` | String | Unique telemetry event identifier | Required | Valid UUID v4 | `"77128441-2990-11ec-abcd-001122334455"` |
| `event_type` | String | Interaction type discriminator | Required | Allowed: `"PAGE_VIEW"`, `"PRODUCT_CLICK"`, `"SEARCH_QUERY"`, `"CATEGORY_BROWSE"` | `"PRODUCT_CLICK"` |
| `timestamp` | Long (timestamp-millis) | Milliseconds since UTC Unix epoch | Required | 64-bit integer >= 0 | `1784999920000` |
| `session_id` | String | User active session identifier | Required | Valid UUID v4 | `"sess-88192-3312"` |
| `user_id` | String / Null | Authenticated user identifier | Nullable | Valid UUID v4 if logged in, otherwise null | `"cust-10293"` |
| `device_type` | String | Client hardware classification | Required | Allowed: `"DESKTOP"`, `"MOBILE"`, `"TABLET"` | `"MOBILE"` |
| `ip_address` | String | Client IP address for geolocation | Required | Valid IPv4 or IPv6 string | `"189.100.22.15"` |
| `user_agent` | String | HTTP User-Agent string | Required | Non-empty string | `"Mozilla/5.0 (iPhone; CPU OS 17_5...)"` |
| `page_url` | String | Fully qualified target URL | Required | Valid URL format | `"https://marketplace.com/products/prod-77123"` |
| `referrer_url` | String / Null | HTTP Referrer / Previous page URL | Nullable | Valid URL format or null | `"https://marketplace.com/categories/electronics"` |
| `product_id` | String / Null | Viewed/clicked product identifier | Nullable | Valid UUID v4 if product event | `"prod-77123"` |
| `category_id` | String / Null | Viewed category identifier | Nullable | Valid UUID v4 if category event | `"cat-elec-01"` |
| `search_term` | String / Null | Query term entered in search bar | Nullable | Non-null only when `event_type` is `"SEARCH_QUERY"` | `null` |

#### Canonical JSON Payload Sample

```json
{
  "event_id": "77128441-2990-11ec-abcd-001122334455",
  "event_type": "PRODUCT_CLICK",
  "timestamp": 1784999920000,
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
* **Schema Subject**: `marketplace.catalog-value`
* **Avro Schema File**: [catalog_changes.avsc](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/schemas/avro/catalog_changes.avsc)
* **Business Domain**: Product Catalog & Merchant Administration Subsystem
* **Trigger Conditions**: Emitted when a seller or catalog admin creates a new product, updates details, changes price, or deactivates a listing.
* **Semantic Meaning**: Signals downstream search indexers, product cache managers, and price tracking engines to re-index or adjust product availability.
* **Partition Key**: `product_id`

#### Attribute Table

| Field Name | Type | Description | Required / Nullable | Validation Rules & Constraints | Example Value |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `event_id` | String | Unique telemetry event identifier | Required | Valid UUID v4 | `"c9981234-4412-9901-0012-aabbccddeeff"` |
| `event_type` | String | Catalog action discriminator | Required | Allowed: `"PRODUCT_CREATED"`, `"PRODUCT_UPDATED"`, `"PRICE_CHANGED"`, `"PRODUCT_DEACTIVATED"` | `"PRICE_CHANGED"` |
| `timestamp` | Long (timestamp-millis) | Milliseconds since UTC Unix epoch | Required | 64-bit integer >= 0 | `1784995200000` |
| `product_id` | String | Unique catalog product identifier | Required | Valid UUID v4 | `"prod-77123"` |
| `seller_id` | String | Merchant owning the product | Required | Valid UUID v4 | `"sell-99201"` |
| `sku` | String | Merchant Stock Keeping Unit code | Required | Non-empty alphanumeric string | `"AUDIO-HD-WIFI-01"` |
| `product_name` | String | Commercial title of the product | Required | Non-empty string | `"Wireless Headphones Noise Cancelling"` |
| `category_id` | String | Primary category identifier | Required | Valid UUID v4 | `"cat-elec-01"` |
| `brand` | String / Null | Brand or manufacturer name | Nullable | Optional string or null | `"AudioTech"` |
| `price` | Decimal (18,4) | Monetary value represented as fixed-precision decimal (precision 18, scale 4) | Required | Must be `> 0.0000` | `125.0000` |
| `previous_price` | Decimal (18,4) / Null | Monetary value represented as fixed-precision decimal (precision 18, scale 4) | Nullable | Must be `> 0.0000`; null if `"PRODUCT_CREATED"` | `149.9900` |
| `is_active` | Boolean | Catalog availability flag | Required | `true` or `false` | `true` |
| `updated_by` | String | User or system agent making edit | Required | Non-empty user identifier | `"user-admin-04"` |

#### Canonical JSON Payload Sample

```json
{
  "event_id": "c9981234-4412-9901-0012-aabbccddeeff",
  "event_type": "PRICE_CHANGED",
  "timestamp": 1784995200000,
  "product_id": "prod-77123",
  "seller_id": "sell-99201",
  "sku": "AUDIO-HD-WIFI-01",
  "product_name": "Wireless Headphones Noise Cancelling",
  "category_id": "cat-elec-01",
  "brand": "AudioTech",
  "price": 125.0000,
  "previous_price": 149.9900,
  "is_active": true,
  "updated_by": "user-admin-04"
}
```

---

## 3. Cross-Event Comparison Matrix

| Event Name | Topic Name | Subject Name | Partition Key | Throughput Profile | Main Downstream Consumers |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `order_creation` | `marketplace.orders` | `marketplace.orders-value` | `order_id` | Medium Volume / High Priority | Ingestion Pipeline, Inventory, Payment, Medallion Bronze |
| `payment_updates` | `marketplace.payments` | `marketplace.payments-value` | `payment_id` | Medium Volume / High Priority | Ingestion Pipeline, Order Status Worker, Medallion Bronze |
| `cart_events` | `marketplace.cart` | `marketplace.cart-value` | `cart_id` | High Volume / Medium Priority | Ingestion Pipeline, Cart Abandonment Analytics, Medallion Bronze |
| `stock_updates` | `marketplace.inventory` | `marketplace.inventory-value` | `product_id` | High Volume / High Priority | Ingestion Pipeline, Catalog Availability, Medallion Bronze |
| `user_navigation` | `marketplace.navigation` | `marketplace.navigation-value` | `session_id` | Ultra-High Volume / Low Latency | Ingestion Pipeline, Clickstream Analytics, Medallion Bronze |
| `catalog_changes` | `marketplace.catalog` | `marketplace.catalog-value` | `product_id` | Low Volume / Medium Priority | Search Engine Indexer, Redis Cache, Medallion Bronze |

---

## 4. Next Steps & References

- **Active Refinement Plan**: Tracked in [docs/context/current-step.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/context/current-step.md).
- **Formal Data Contracts Specification**: Refer to [docs/business_domain_modeling.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/business_domain_modeling.md).
- **Avro Schemas Directory**: Explore schemas under [ingestion/schemas/avro/](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/schemas/avro/).
- **Schema Registry Manager**: Code in [ingestion/schema_registry.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/schema_registry.py).
- **Avro Serialization Module**: Code in [ingestion/serialization.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/serialization.py).
- **Schema Registration CLI**: Executable script at [scripts/register_schemas.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/scripts/register_schemas.py).
