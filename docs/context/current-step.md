# Current Step: Step 3 — Building Event Producers

## Active Tasks

### Phase 1: Ingestion Infrastructure & Base Producer Framework
- [x] Implement the abstract base producer class `BaseProducer` in [ingestion/producers/base_producer.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/producers/base_producer.py) providing Kafka client configuration, Confluent Schema Registry wire-format serialization via [ingestion/serialization.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/serialization.py), rate limiting, delivery callbacks, and thread-safe execution
    * **Assigned Agent**: `software-engineer-agent`
- [x] Build the synthetic data generation engine in [ingestion/producers/generator.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/producers/generator.py) using `Faker` and custom generators to generate domain entity data, handling updated Avro logical types (`timestamp-millis` long integers and `decimal(18,4)` Decimal objects)
    * **Assigned Agent**: `data-engineer-agent`
- [x] Build producer configuration and CLI entry point utilities in [ingestion/producers/config.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/producers/config.py) to manage Kafka broker parameters, Schema Registry settings, topic mappings, target event throughput rate, and execution runtime
    * **Assigned Agent**: `software-engineer-agent`

### Phase 2: Domain Producers Implementation
- [ ] Implement `CheckoutProducer` in [ingestion/producers/checkout_producer.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/producers/checkout_producer.py) for publishing `order_creation` events to `marketplace.orders` and `cart_events` to `marketplace.cart` with valid entity identifiers and monetary fields
    * **Assigned Agent**: `data-engineer-agent`
- [ ] Implement `PaymentProducer` in [ingestion/producers/payment_producer.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/producers/payment_producer.py) for publishing `payment_updates` events to `marketplace.payments` with realistic payment gateway status transitions (`AUTHORIZED`, `APPROVED`, `DECLINED`, `REFUNDED`, `FAILED`)
    * **Assigned Agent**: `data-engineer-agent`
- [ ] Implement `NavigationProducer` in [ingestion/producers/navigation_producer.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/producers/navigation_producer.py) emitting high-volume clickstream events (`user_navigation`) to `marketplace.navigation` simulating user sessions, search queries, and page interactions
    * **Assigned Agent**: `data-engineer-agent`
- [ ] Implement `InventoryProducer` in [ingestion/producers/inventory_producer.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/producers/inventory_producer.py) emitting `stock_updates` to `marketplace.inventory` and `catalog_changes` to `marketplace.catalog` covering stock reservations, replenishment, and product updates
    * **Assigned Agent**: `data-engineer-agent`
- [ ] Implement `AntiFraudProducer` in [ingestion/producers/antifraud_producer.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/producers/antifraud_producer.py) emitting security telemetry and generating suspicious activity patterns (rapid checkout frequency, geographical anomalies, unusual high-value orders)
    * **Assigned Agent**: `software-engineer-agent`

### Phase 3: Chaos & Resilience Simulation Scenarios
- [ ] Implement `ChaosEngine` in [ingestion/producers/chaos.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/producers/chaos.py) to simulate traffic bursts, variable volume, and delayed or out-of-order events with altered `timestamp-millis` values
    * **Assigned Agent**: `software-engineer-agent`
- [ ] Extend `ChaosEngine` in [ingestion/producers/chaos.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/producers/chaos.py) to inject duplicate event payloads, schema-invalid payloads for Dead Letter Queue testing, and simulate intermittent Kafka connection failures with retry handling
    * **Assigned Agent**: `software-engineer-agent`

### Phase 4: Integration Testing & Verification
- [ ] Create unit and component pytest test suites in [tests/test_producers.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/tests/test_producers.py) and [tests/test_chaos.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/tests/test_chaos.py) testing event generation, serialization, rate limiting, and chaos scenario generators
    * **Assigned Agent**: `software-engineer-agent`
- [ ] Create CLI simulation script [scripts/run_producer_simulation.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/scripts/run_producer_simulation.py) and end-to-end integration test in [tests/test_live_producers.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/tests/test_live_producers.py) executing real-time streaming against local Kafka broker and Schema Registry
    * **Assigned Agent**: `data-engineer-agent`

### Phase 5: Documentation
- [ ] Write comprehensive technical documentation for event producers, architecture, CLI parameters, and chaos simulation scenarios in [docs/event_producers_guide.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/event_producers_guide.md)
    * **Assigned Agent**: `tech-writer-agent`
- [ ] Create session log and architecture decision updates under [docs/learning/step-03-building-event-producers/](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/learning/step-03-building-event-producers/) documenting producer design trade-offs and simulation findings
    * **Assigned Agent**: `tech-writer-agent`
