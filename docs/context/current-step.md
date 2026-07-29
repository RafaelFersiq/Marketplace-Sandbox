# Current Step: Step 2 Refinement — Schema Registry Caching & Avro Logical Types

## Active Tasks

### Phase 1: Schema Registry Client & Caching Refinements
- [x] Add an in-memory LRU cache (`_schema_id_cache`) to `SchemaRegistryManager.get_schema_by_id()` in [ingestion/schema_registry.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/schema_registry.py) to prevent redundant HTTP requests during payload deserialization
    * **Assigned Agent**: `software-engineer-agent`
- [x] Implement compatibility level configuration methods (`set_compatibility_level`, `get_compatibility_level`) in [ingestion/schema_registry.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/schema_registry.py) and update [scripts/register_schemas.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/scripts/register_schemas.py) to enforce `BACKWARD` compatibility on all subjects
    * **Assigned Agent**: `software-engineer-agent`


### Phase 2: Avro Logical Types & Precision Evolution
- [x] Update all 6 Avro schemas under [ingestion/schemas/avro/](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/schemas/avro/) to use Avro logicalType for timestamps (`{"type": "long", "logicalType": "timestamp-millis"}`) instead of ISO 8601 strings
    * **Assigned Agent**: `data-engineer-agent`
- [x] Refactor financial monetary fields across all schemas (`total_amount`, `subtotal_amount`, `unit_price`, `shipping_fee`, `discount_amount`, `previous_price`, `cart_total_value`, `amount`, `price`, `subtotal`) to use Avro `decimal` logical types (`{"type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 4}`) to eliminate floating-point imprecision
    * **Assigned Agent**: `data-engineer-agent`

### Phase 3: Serialization Module & Test Suite Update
- [x] Update [ingestion/serialization.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/serialization.py) to seamlessly handle Python `datetime` objects and `decimal.Decimal` instances during Avro encoding/decoding
    * **Assigned Agent**: `software-engineer-agent`
- [x] Update and expand pytest test suites ([tests/test_schema_registry.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/tests/test_schema_registry.py), [tests/test_serialization.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/tests/test_serialization.py)) to test LRU cache hits, compatibility enforcement, timestamp-millis conversion, and decimal precision serialization
    * **Assigned Agent**: `software-engineer-agent`

### Phase 4: Formal Documentation Consolidation
- [x] Update formal documentation in [docs/business_domain_modeling.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/business_domain_modeling.md) with updated Avro logical types, caching strategy, and compatibility rules
    * **Assigned Agent**: `tech-writer-agent`

