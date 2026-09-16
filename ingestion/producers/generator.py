"""Synthetic marketplace data generator engine.

Provides SyntheticDataGenerator utilizing Faker and relational state pools to produce
realistic Avro-compliant marketplace event payloads with logical types (timestamp-millis, decimal).
"""

import random
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from faker import Faker


class SyntheticDataGenerator:
    """Generates synthetic domain events for marketplace streaming simulations."""

    def __init__(self, seed: int | None = None, locale: str = "pt_BR") -> None:
        """Initialize generator with pools of realistic entity references.

        Args:
            seed: Optional integer seed for reproducible random generation.
            locale: Faker locale (defaults to pt_BR for marketplace context).
        """
        self.seed = seed
        self.fake = Faker(locale)
        if seed is not None:
            self.fake.seed_instance(seed)
            random.seed(seed)

        self._categories: list[dict[str, str]] = [
            {"category_id": str(uuid.uuid4()), "name": "Eletrônicos"},
            {"category_id": str(uuid.uuid4()), "name": "Informática"},
            {"category_id": str(uuid.uuid4()), "name": "Eletrodomésticos"},
            {"category_id": str(uuid.uuid4()), "name": "Moda & Acessórios"},
            {"category_id": str(uuid.uuid4()), "name": "Casa & Decoração"},
            {"category_id": str(uuid.uuid4()), "name": "Esporte & Lazer"},
            {"category_id": str(uuid.uuid4()), "name": "Beleza & Perfumaria"},
            {"category_id": str(uuid.uuid4()), "name": "Livros"},
        ]

        self._warehouses: list[str] = [
            "WH-SP-01",
            "WH-SP-02",
            "WH-RJ-01",
            "WH-MG-01",
            "WH-SC-01",
            "WH-BA-01",
        ]

        self._sellers: list[dict[str, str]] = [
            {"seller_id": str(uuid.uuid4()), "name": self.fake.company()} for _ in range(15)
        ]

        self._customers: list[dict[str, Any]] = [
            {
                "customer_id": str(uuid.uuid4()),
                "name": self.fake.name(),
                "email": self.fake.email(),
                "address": {
                    "street": self.fake.street_name(),
                    "number": str(self.fake.building_number()),
                    "complement": random.choice([None, f"Apto {random.randint(10, 999)}", "Bloco B"]),
                    "city": self.fake.city(),
                    "state": self.fake.state_abbr(),
                    "zip_code": self.fake.postcode().replace("-", ""),
                    "country": "BR",
                },
            }
            for _ in range(50)
        ]

        self._products: list[dict[str, Any]] = []
        sample_items = [
            ("Smartphone Galaxy S24", "Samsung", 4899.90, "Eletrônicos"),
            ("iPhone 15 Pro", "Apple", 7999.00, "Eletrônicos"),
            ("Notebook Dell XPS 13", "Dell", 6499.00, "Informática"),
            ("Monitor Gamer 27 144Hz", "LG", 1299.99, "Informática"),
            ("Smart TV 55 4K", "LG", 2799.00, "Eletrodomésticos"),
            ("Tênis Air Max", "Nike", 599.90, "Esporte & Lazer"),
            ("Camiseta Dry-Fit", "Adidas", 129.90, "Moda & Acessórios"),
            ("Cafeteira Elétrica", "Philips", 189.90, "Eletrodomésticos"),
            ("Mouse Sem Fio MX Master", "Logitech", 499.90, "Informática"),
            ("Jogo de Panelas Inox", "Tramontina", 349.90, "Casa & Decoração"),
        ]
        for name, brand, base_price, cat_name in sample_items:
            cat = next(c for c in self._categories if c["name"] == cat_name)
            seller = random.choice(self._sellers)
            price_dec = Decimal(str(base_price)).quantize(Decimal("0.0001"))
            self._products.append(
                {
                    "product_id": str(uuid.uuid4()),
                    "seller_id": seller["seller_id"],
                    "category_id": cat["category_id"],
                    "sku": f"SKU-{random.randint(100000, 999999)}",
                    "product_name": name,
                    "brand": brand,
                    "price": price_dec,
                    "is_active": True,
                }
            )

        self._active_sessions: list[dict[str, Any]] = [
            {
                "session_id": str(uuid.uuid4()),
                "user_id": random.choice(self._customers)["customer_id"] if random.random() > 0.3 else None,
                "ip_address": self.fake.ipv4(),
                "user_agent": self.fake.user_agent(),
                "device_type": random.choice(["DESKTOP", "MOBILE", "TABLET"]),
            }
            for _ in range(30)
        ]

        self._active_orders: list[dict[str, Any]] = []

    def _now(self) -> datetime:
        """Return current timestamp in UTC."""
        return datetime.now(UTC)

    def generate_order_creation(self) -> dict[str, Any]:
        """Generate an OrderCreationEvent payload."""
        customer = random.choice(self._customers)
        seller = random.choice(self._sellers)
        order_id = str(uuid.uuid4())
        event_id = str(uuid.uuid4())

        # Select 1 to 4 items
        num_items = random.randint(1, 4)
        selected_products = random.sample(self._products, min(num_items, len(self._products)))

        items: list[dict[str, Any]] = []
        subtotal_sum = Decimal("0.0000")
        for prod in selected_products:
            qty = random.randint(1, 3)
            unit_price = prod["price"]
            item_subtotal = (unit_price * Decimal(qty)).quantize(Decimal("0.0001"))
            subtotal_sum += item_subtotal
            items.append(
                {
                    "item_id": str(uuid.uuid4()),
                    "product_id": prod["product_id"],
                    "product_name": prod["product_name"],
                    "quantity": qty,
                    "unit_price": unit_price,
                    "subtotal": item_subtotal,
                }
            )

        discount = Decimal(str(random.choice([0.0, 10.0, 25.0, 50.0]))).quantize(Decimal("0.0001"))
        shipping = Decimal(str(random.choice([0.0, 15.90, 24.50, 39.90]))).quantize(Decimal("0.0001"))
        total = (subtotal_sum + shipping - discount).quantize(Decimal("0.0001"))
        if total < Decimal("0.0000"):
            total = subtotal_sum

        payment_method = random.choice(["CREDIT_CARD", "PIX", "BOLETO", "DEBIT_CARD"])
        order_payload = {
            "event_id": event_id,
            "event_type": "ORDER_CREATED",
            "timestamp": self._now(),
            "order_id": order_id,
            "customer_id": customer["customer_id"],
            "seller_id": seller["seller_id"],
            "order_status": "CREATED",
            "currency": "BRL",
            "subtotal_amount": subtotal_sum,
            "discount_amount": discount,
            "shipping_fee": shipping,
            "total_amount": total,
            "payment_method": payment_method,
            "shipping_address": {
                "street": customer["address"]["street"],
                "number": customer["address"]["number"],
                "complement": customer["address"]["complement"],
                "city": customer["address"]["city"],
                "state": customer["address"]["state"],
                "zip_code": customer["address"]["zip_code"],
                "country": customer["address"]["country"],
            },
            "items": items,
        }

        # Keep in pool for correlation
        self._active_orders.append(
            {
                "order_id": order_id,
                "customer_id": customer["customer_id"],
                "total_amount": total,
                "payment_method": payment_method,
                "items": items,
            }
        )
        if len(self._active_orders) > 200:
            self._active_orders.pop(0)

        return order_payload

    def generate_payment_update(self) -> dict[str, Any]:
        """Generate a PaymentUpdateEvent payload."""
        if self._active_orders and random.random() > 0.2:
            matched_order = random.choice(self._active_orders)
            order_id = matched_order["order_id"]
            customer_id = matched_order["customer_id"]
            amount = matched_order["total_amount"]
            payment_method = matched_order["payment_method"]
        else:
            order_id = str(uuid.uuid4())
            customer_id = random.choice(self._customers)["customer_id"]
            amount = Decimal(str(random.randint(50, 2500))).quantize(Decimal("0.0001"))
            payment_method = random.choice(["CREDIT_CARD", "PIX", "BOLETO", "DEBIT_CARD"])

        status_choices = ["APPROVED", "AUTHORIZED", "DECLINED", "FAILED", "REFUNDED"]
        status_weights = [0.75, 0.10, 0.08, 0.04, 0.03]
        status = random.choices(status_choices, weights=status_weights, k=1)[0]

        failure_reason = None
        if status in ("DECLINED", "FAILED"):
            failure_reason = random.choice(
                [
                    "INSUFFICIENT_FUNDS",
                    "CARD_EXPIRED",
                    "FRAUD_SUSPICION",
                    "GATEWAY_TIMEOUT",
                ]
            )

        installments = 1 if payment_method in ("PIX", "BOLETO", "DEBIT_CARD") else random.randint(1, 10)

        return {
            "event_id": str(uuid.uuid4()),
            "event_type": "PAYMENT_STATUS_UPDATED",
            "timestamp": self._now(),
            "payment_id": str(uuid.uuid4()),
            "order_id": order_id,
            "customer_id": customer_id,
            "payment_method": payment_method,
            "payment_status": status,
            "amount": amount,
            "currency": "BRL",
            "gateway_provider": random.choice(["STRIPE", "PAGSEGURO", "MOCK_GATEWAY"]),
            "transaction_reference": f"TXN-{random.randint(10000000, 99999999)}",
            "failure_reason": failure_reason,
            "installments": installments,
        }

    def generate_cart_event(self) -> dict[str, Any]:
        """Generate a CartEvent payload."""
        session = random.choice(self._active_sessions)
        event_type = random.choice(
            [
                "CART_ITEM_ADDED",
                "CART_ITEM_ADDED",
                "CART_QUANTITY_UPDATED",
                "CART_ITEM_REMOVED",
                "CART_CLEARED",
            ]
        )

        if event_type == "CART_CLEARED":
            product_id = None
            quantity = None
            unit_price = None
            cart_total_items = 0
            cart_total_value = Decimal("0.0000")
        else:
            product = random.choice(self._products)
            product_id = product["product_id"]
            quantity = random.randint(1, 3)
            unit_price = product["price"]
            cart_total_items = random.randint(1, 6)
            cart_total_value = (unit_price * Decimal(cart_total_items)).quantize(Decimal("0.0001"))

        return {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "timestamp": self._now(),
            "cart_id": str(uuid.uuid4()),
            "user_id": session["user_id"],
            "session_id": session["session_id"],
            "product_id": product_id,
            "quantity": quantity,
            "unit_price": unit_price,
            "cart_total_items": cart_total_items,
            "cart_total_value": cart_total_value,
        }

    def generate_stock_update(self) -> dict[str, Any]:
        """Generate a StockUpdateEvent payload."""
        product = random.choice(self._products)
        event_type = random.choice(
            [
                "STOCK_RESERVED",
                "STOCK_RELEASED",
                "STOCK_REPLENISHED",
                "STOCK_ADJUSTED",
            ]
        )

        prev_stock = random.randint(10, 200)
        if event_type in ("STOCK_RESERVED", "STOCK_RELEASED"):
            change = -random.randint(1, 5) if event_type == "STOCK_RESERVED" else random.randint(1, 5)
            reason = "ORDER_RESERVATION" if change < 0 else "CANCELLED_ORDER"
            order_id = str(uuid.uuid4())
        elif event_type == "STOCK_REPLENISHED":
            change = random.randint(20, 100)
            reason = "REPLENISHMENT"
            order_id = None
        else:
            change = random.choice([-2, -1, 1, 2])
            reason = "DAMAGED_ITEM" if change < 0 else "INVENTORY_COUNT"
            order_id = None

        new_stock = max(0, prev_stock + change)

        return {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "timestamp": self._now(),
            "inventory_id": str(uuid.uuid4()),
            "product_id": product["product_id"],
            "seller_id": product["seller_id"],
            "warehouse_id": random.choice(self._warehouses),
            "quantity_change": change,
            "previous_stock": prev_stock,
            "new_stock": new_stock,
            "reason": reason,
            "order_id": order_id,
        }

    def generate_catalog_change(self) -> dict[str, Any]:
        """Generate a CatalogChangeEvent payload."""
        product = random.choice(self._products)
        event_type = random.choice(
            [
                "PRODUCT_CREATED",
                "PRODUCT_UPDATED",
                "PRICE_CHANGED",
                "PRODUCT_DEACTIVATED",
            ]
        )

        prev_price: Decimal | None = None
        price = product["price"]
        is_active = True

        if event_type == "PRICE_CHANGED":
            prev_price = price
            price_variation = random.choice([Decimal("-10.0000"), Decimal("20.0000"), Decimal("-5.5000")])
            price = max(Decimal("1.0000"), (price + price_variation).quantize(Decimal("0.0001")))
        elif event_type == "PRODUCT_DEACTIVATED":
            is_active = False

        return {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "timestamp": self._now(),
            "product_id": product["product_id"],
            "seller_id": product["seller_id"],
            "sku": product["sku"],
            "product_name": product["product_name"],
            "category_id": product["category_id"],
            "brand": product["brand"],
            "price": price,
            "previous_price": prev_price,
            "is_active": is_active,
            "updated_by": f"seller_portal_{random.randint(100, 999)}",
        }

    def generate_user_navigation(self) -> dict[str, Any]:
        """Generate a UserNavigationEvent payload."""
        session = random.choice(self._active_sessions)
        event_type = random.choice(
            [
                "PAGE_VIEW",
                "PRODUCT_CLICK",
                "SEARCH_QUERY",
                "CATEGORY_BROWSE",
            ]
        )

        product = random.choice(self._products) if event_type == "PRODUCT_CLICK" else None
        category = random.choice(self._categories) if event_type == "CATEGORY_BROWSE" else None
        search_terms = ["smartphone", "fone bluetooth", "tenis nike", "notebook i7", "monitor gamer", "cafeteira"]
        search_term = random.choice(search_terms) if event_type == "SEARCH_QUERY" else None

        page_url = "https://marketplace.com"
        if event_type == "PAGE_VIEW":
            page_url = "https://marketplace.com/home"
        elif event_type == "PRODUCT_CLICK" and product:
            page_url = f"https://marketplace.com/products/{product['product_id']}"
        elif event_type == "SEARCH_QUERY" and search_term:
            page_url = f"https://marketplace.com/search?q={search_term.replace(' ', '+')}"
        elif event_type == "CATEGORY_BROWSE" and category:
            page_url = f"https://marketplace.com/categories/{category['category_id']}"

        return {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "timestamp": self._now(),
            "session_id": session["session_id"],
            "user_id": session["user_id"],
            "device_type": session["device_type"],
            "ip_address": session["ip_address"],
            "user_agent": session["user_agent"],
            "page_url": page_url,
            "referrer_url": random.choice([None, "https://google.com", "https://marketplace.com/home"]),
            "product_id": product["product_id"] if product else None,
            "category_id": category["category_id"] if category else None,
            "search_term": search_term,
        }
