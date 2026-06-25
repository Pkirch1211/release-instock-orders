import csv
import json
import logging
import os
import re
import sys
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

load_dotenv()

# =========================
# ENV
# =========================

SHOPIFY_SHOP = os.getenv("SHOPIFY_SHOP", "").strip()
SHOPIFY_TOKEN = os.getenv("SHOPIFY_TOKEN", "").strip()
SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2025-07").strip()
SHOPIFY_LOCATION_ID = os.getenv("SHOPIFY_LOCATION_ID", "").strip()

READY_TAG = os.getenv("READY_TAG", "instock-ready").strip()
PROCESSING_TAG = os.getenv("PROCESSING_TAG", "order-push-processing").strip()
SUBMITTED_TAG = os.getenv("SUBMITTED_TAG", "order-submitted").strip()
NEEDS_REVIEW_TAG = os.getenv("NEEDS_REVIEW_TAG", "needs-review").strip()
LOW_SUPPLY_TAG = os.getenv("LOW_SUPPLY_TAG", "low-supply").strip()
INVENTORY_SHORTAGE_TAG = os.getenv("INVENTORY_SHORTAGE_TAG", "inventory-shortage").strip()

INVENTORY_REVIEW_NAMESPACE = os.getenv("INVENTORY_REVIEW_NAMESPACE", "b2b").strip()
INVENTORY_REVIEW_KEY = os.getenv("INVENTORY_REVIEW_KEY", "inventory_review_reason").strip()
INVENTORY_REVIEW_TYPE = os.getenv("INVENTORY_REVIEW_TYPE", "multi_line_text_field").strip()

SHIP_DATE_NAMESPACE = os.getenv("SHIP_DATE_NAMESPACE", "b2b").strip()
SHIP_DATE_KEY = os.getenv("SHIP_DATE_KEY", "ship_date").strip()

EXCLUDE_TAGS = {
    t.strip()
    for t in os.getenv(
        "EXCLUDE_TAGS",
        "split-backorder-child,split-backorder-processing,needs-review",
    ).split(",")
    if t.strip()
}
EXCLUDE_TAGS.add(NEEDS_REVIEW_TAG)


COMPLETE_DRAFT_NAMES = {
    name.strip().replace("#", "")
    for name in os.getenv("COMPLETE_DRAFT_NAMES", "").split(",")
    if name.strip()
}

# If set, ONLY these draft names will be processed. All others are skipped.
# Accepts comma-separated list with or without leading #.
# Leave unset or empty for normal operation (process all eligible drafts).
PROCESS_ONLY_DRAFTS = {
    name.strip().replace("#", "").upper()
    for name in os.getenv("PROCESS_ONLY_DRAFTS", "").split(",")
    if name.strip()
}

# IMPORTANT:
# No fallback defaults here. If you do not explicitly set these env vars,
# nothing is excluded by customer name.
EXCLUDED_CUSTOMERS = {
    c.strip().upper()
    for c in os.getenv("EXCLUDED_CUSTOMERS", "").split(",")
    if c.strip()
}

EXCLUDED_CUSTOMER_SUBSTRINGS = {
    c.strip().upper()
    for c in os.getenv("EXCLUDED_CUSTOMER_SUBSTRINGS", "").split(",")
    if c.strip()
}

EXCLUDED_SKUS = {
    sku.strip().upper()
    for sku in os.getenv("EXCLUDED_SKUS", "").split(",")
    if sku.strip()
}

# Where to write a JSON snapshot of EXCLUDED_SKUS (with product titles) for
# the Ops Scorecard dashboard to read. Purely additive / side-channel —
# never read back by this script, so it cannot affect order processing.
EXCLUDED_SKUS_EXPORT_PATH = os.getenv("EXCLUDED_SKUS_EXPORT_PATH", "excluded_skus.json").strip()

DRY_RUN = os.getenv("DRY_RUN", "true").strip().lower() == "true"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper().strip()
DRAFTS_PAGE_SIZE = int(os.getenv("DRAFTS_PAGE_SIZE", "25").strip())
INVENTORY_REVIEW_THRESHOLD = int(os.getenv("INVENTORY_REVIEW_THRESHOLD", "350").strip())
INVENTORY_BATCH_SIZE = int(os.getenv("INVENTORY_BATCH_SIZE", "25").strip())
INVENTORY_LEVELS_PAGE_SIZE = int(os.getenv("INVENTORY_LEVELS_PAGE_SIZE", "250").strip())

# In-run inventory reservation pool. This prevents two drafts in the same run
# from both passing against the same starting available quantity.
# Shape: {inventory_item_id: {"sku": str, "available": Optional[int]}}
InventoryPool = Dict[str, Dict[str, Optional[int]]]

CSV_LOG_PATH = os.getenv("CSV_LOG_PATH", "release_instock_orders_log.csv").strip()
FREIGHT_RATE_PERCENT = Decimal(os.getenv("FREIGHT_RATE_PERCENT", "12").strip())
DEFAULT_FREIGHT_TITLE = os.getenv("DEFAULT_FREIGHT_TITLE", "UPS Ground").strip() or "UPS Ground"

DEFAULT_PAYMENT_TERMS_TEMPLATE_ID = os.getenv(
    "DEFAULT_PAYMENT_TERMS_TEMPLATE_ID",
    "gid://shopify/PaymentTermsTemplate/4",
).strip()

PAYMENT_TEMPLATE_MAP: Dict[int, str] = {
    30: os.getenv("PAYMENT_TERMS_TEMPLATE_ID_NET30", "").strip() or DEFAULT_PAYMENT_TERMS_TEMPLATE_ID,
    45: os.getenv("PAYMENT_TERMS_TEMPLATE_ID_NET45", "").strip(),
    60: os.getenv("PAYMENT_TERMS_TEMPLATE_ID_NET60", "").strip(),
    90: os.getenv("PAYMENT_TERMS_TEMPLATE_ID_NET90", "").strip(),
    120: os.getenv("PAYMENT_TERMS_TEMPLATE_ID_NET120", "").strip(),
}

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("mt_shopify_sync.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

if not SHOPIFY_SHOP or not SHOPIFY_TOKEN or not SHOPIFY_LOCATION_ID:
    logger.error(
        "Missing required environment variables: SHOPIFY_SHOP, SHOPIFY_TOKEN, and/or SHOPIFY_LOCATION_ID"
    )
    sys.exit(1)

GRAPHQL_URL = f"https://{SHOPIFY_SHOP}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"
HEADERS = {
    "X-Shopify-Access-Token": SHOPIFY_TOKEN,
    "Content-Type": "application/json",
}

CSV_HEADERS = [
    "pushed_at",
    "dry_run",
    "draft_id",
    "draft_name",
    "po_number",
    "company",
    "status",
    "action",
    "success",
    "reason",
    "detected_terms",
    "existing_terms_before",
    "payment_terms_after",
    "ship_date",
    "freight_action",
    "freight_title",
    "freight_price",
    "final_tags",
    "order_id",
    "order_name",
]

# =========================
# GRAPHQL
# =========================

CANDIDATE_DRAFTS_QUERY = """
query CandidateDrafts(
  $cursor: String,
  $pageSize: Int!,
  $shipDateNamespace: String!,
  $shipDateKey: String!
) {
  draftOrders(
    first: $pageSize,
    after: $cursor,
    query: "status:open tag:instock-ready -tag:needs-review -tag:order-push-processing -tag:order-submitted"
  ) {
    edges {
      cursor
      node {
        id
        name
        status
        tags
        note2
        poNumber
        updatedAt
        currencyCode
        subtotalPriceSet {
          shopMoney {
            amount
            currencyCode
          }
        }
        shippingLine {
          id
          title
          custom
          discountedPriceSet {
            shopMoney {
              amount
              currencyCode
            }
          }
        }
        lineItems(first: 250) {
          edges {
            node {
              sku
              title
              quantity
              variant {
                id
                displayName
                inventoryItem {
                  id
                  tracked
                  sku
                }
              }
            }
          }
        }
        purchasingEntity {
          __typename
          ... on Customer {
            displayName
          }
          ... on PurchasingCompany {
            company {
              name
            }
            location {
              name
            }
          }
        }
        order {
          id
          name
          tags
        }
        paymentTerms {
          id
          dueInDays
          translatedName
          paymentTermsName
        }
        metafield(namespace: $shipDateNamespace, key: $shipDateKey) {
          value
          type
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

# FIX: Added variant.inventoryItem to line items so excluded SKU checks
# and inventory threshold checks work correctly on rechecked drafts,
# matching CANDIDATE_DRAFTS_QUERY.
DRAFT_RECHECK_QUERY = """
query RecheckDraft(
  $id: ID!,
  $shipDateNamespace: String!,
  $shipDateKey: String!
) {
  draftOrder(id: $id) {
    id
    name
    status
    tags
    note2
    poNumber
    currencyCode
    subtotalPriceSet {
      shopMoney {
        amount
        currencyCode
      }
    }
    shippingLine {
      id
      title
      custom
      discountedPriceSet {
        shopMoney {
          amount
          currencyCode
        }
      }
    }
    lineItems(first: 250) {
      edges {
        node {
          sku
          title
          quantity
          variant {
            id
            displayName
            inventoryItem {
              id
              tracked
              sku
            }
          }
        }
      }
    }
    purchasingEntity {
      __typename
      ... on Customer {
        displayName
      }
      ... on PurchasingCompany {
        company {
          name
        }
        location {
          name
        }
      }
    }
    order {
      id
      name
      tags
    }
    paymentTerms {
      id
      dueInDays
      translatedName
      paymentTermsName
    }
    metafield(namespace: $shipDateNamespace, key: $shipDateKey) {
      value
      type
    }
  }
}
"""

ORDER_RECHECK_QUERY = """
query RecheckOrder($id: ID!) {
  order(id: $id) {
    id
    name
    displayFulfillmentStatus
    displayFinancialStatus
    tags
  }
}
"""

INVENTORY_ITEMS_QUERY = """
query GetInventoryItems($ids: [ID!]!, $levelsPageSize: Int!) {
  nodes(ids: $ids) {
    ... on InventoryItem {
      id
      tracked
      sku
      inventoryLevels(first: $levelsPageSize) {
        edges {
          node {
            location {
              id
              name
            }
            quantities(names: ["available"]) {
              name
              quantity
            }
          }
        }
      }
    }
  }
}
"""

# Used only by publish_excluded_skus_snapshot() to attach a human-readable
# product title to each excluded SKU for the dashboard. Not used anywhere
# in the order-processing flow.
VARIANT_BY_SKU_QUERY = """
query VariantBySku($query: String!) {
  productVariants(first: 5, query: $query) {
    edges {
      node {
        sku
        displayName
        product {
          title
        }
      }
    }
  }
}
"""

DRAFT_UPDATE_MUTATION = """
mutation UpdateDraftOrder($id: ID!, $input: DraftOrderInput!) {
  draftOrderUpdate(id: $id, input: $input) {
    draftOrder {
      id
      name
      tags
      paymentTerms {
        id
        dueInDays
        translatedName
        paymentTermsName
      }
      shippingLine {
        id
        title
        custom
        discountedPriceSet {
          shopMoney {
            amount
            currencyCode
          }
        }
      }
      order {
        id
        name
        tags
      }
    }
    userErrors {
      field
      message
    }
  }
}
"""

ORDER_UPDATE_MUTATION = """
mutation UpdateOrder($input: OrderInput!) {
  orderUpdate(input: $input) {
    order {
      id
      name
      tags
    }
    userErrors {
      field
      message
    }
  }
}
"""

METAFIELDS_SET_MUTATION = """
mutation SetMetafields($metafields: [MetafieldsSetInput!]!) {
  metafieldsSet(metafields: $metafields) {
    metafields {
      id
      namespace
      key
      type
      value
    }
    userErrors {
      field
      message
      code
    }
  }
}
"""

DRAFT_COMPLETE_MUTATION = """
mutation CompleteDraftOrder($id: ID!, $paymentPending: Boolean!) {
  draftOrderComplete(id: $id, paymentPending: $paymentPending) {
    draftOrder {
      id
      name
      order {
        id
        name
        tags
      }
    }
    userErrors {
      field
      message
    }
  }
}
"""

DRAFT_COMPLETE_MUTATION_FREE = """
mutation CompleteDraftOrderFree($id: ID!) {
  draftOrderComplete(id: $id) {
    draftOrder {
      id
      name
      order {
        id
        name
        tags
      }
    }
    userErrors {
      field
      message
    }
  }
}
"""

# =========================
# HELPERS
# =========================

def shopify_graphql(query: str, variables: Optional[dict] = None) -> dict:
    response = requests.post(
        GRAPHQL_URL,
        headers=HEADERS,
        json={"query": query, "variables": variables or {}},
        timeout=60,
    )
    response.raise_for_status()
    payload = response.json()

    if "errors" in payload:
        raise RuntimeError(f"GraphQL errors: {payload['errors']}")

    return payload["data"]


def chunked(items: List[str], size: int) -> List[List[str]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def normalize_tags(tags: List[str]) -> List[str]:
    return sorted({tag.strip() for tag in tags if tag and tag.strip()})


def add_tags(existing_tags: List[str], *tags_to_add: str) -> List[str]:
    merged = set(normalize_tags(existing_tags))
    for tag in tags_to_add:
        if tag and tag.strip():
            merged.add(tag.strip())
    return sorted(merged)


def remove_tags(existing_tags: List[str], *tags_to_remove: str) -> List[str]:
    merged = set(normalize_tags(existing_tags))
    for tag in tags_to_remove:
        if tag and tag.strip():
            merged.discard(tag.strip())
    return sorted(merged)


def has_excluded_tag(tags: List[str]) -> bool:
    return any(tag in EXCLUDE_TAGS for tag in tags)


def normalize_draft_name(name: str) -> str:
    return (name or "").replace("#", "").strip().upper()


def should_process_draft(draft_name: str) -> bool:
    normalized = normalize_draft_name(draft_name)

    # Hard allowlist: if set, only process drafts explicitly listed
    if PROCESS_ONLY_DRAFTS:
        return normalized in PROCESS_ONLY_DRAFTS

    # Soft allowlist: if set, only process drafts in COMPLETE_DRAFT_NAMES
    if COMPLETE_DRAFT_NAMES:
        return normalized in {n.upper() for n in COMPLETE_DRAFT_NAMES}

    return True


def payment_terms_name(payment_terms: Optional[dict]) -> str:
    if not payment_terms:
        return ""
    return (
        payment_terms.get("translatedName")
        or payment_terms.get("paymentTermsName")
        or ""
    )


def safe_company_name(draft: dict) -> str:
    entity = draft.get("purchasingEntity") or {}
    typename = entity.get("__typename")

    if typename == "PurchasingCompany":
        company = entity.get("company") or {}
        location = entity.get("location") or {}
        company_name = (company.get("name") or "").strip()
        location_name = (location.get("name") or "").strip()

        if company_name and location_name:
            return f"{company_name} | {location_name}"
        return company_name or location_name

    if typename == "Customer":
        return (entity.get("displayName") or "").strip()

    return ""


def should_exclude_customer(draft: dict) -> bool:
    customer_name = safe_company_name(draft).strip().upper()
    if not customer_name:
        return False

    if EXCLUDED_CUSTOMERS and customer_name in EXCLUDED_CUSTOMERS:
        return True

    if EXCLUDED_CUSTOMER_SUBSTRINGS:
        return any(fragment in customer_name for fragment in EXCLUDED_CUSTOMER_SUBSTRINGS)

    return False


def draft_line_item_skus(draft: dict) -> List[str]:
    """
    Return SKUs for all line items. Prefers the line item's own sku field,
    but falls back to variant.inventoryItem.sku if the line item sku is blank.
    This ensures excluded SKU checks work even when Shopify omits the top-level
    sku on draft order line items.
    """
    line_items = draft.get("lineItems") or {}
    edges = line_items.get("edges") or []

    skus: List[str] = []
    for edge in edges:
        node = edge.get("node") or {}
        sku = (node.get("sku") or "").strip()

        if not sku:
            variant = node.get("variant") or {}
            inventory_item = variant.get("inventoryItem") or {}
            sku = (inventory_item.get("sku") or "").strip()

        if sku:
            skus.append(sku)

    return skus


def excluded_skus_on_draft(draft: dict) -> List[str]:
    if not EXCLUDED_SKUS:
        return []

    normalized_excluded = {sku.upper() for sku in EXCLUDED_SKUS}
    matched = {
        sku
        for sku in draft_line_item_skus(draft)
        if sku.strip().upper() in normalized_excluded
    }
    return sorted(matched, key=lambda value: value.upper())


def should_exclude_sku(draft: dict) -> bool:
    return bool(excluded_skus_on_draft(draft))


def draft_inventory_item_ids(draft: dict) -> List[str]:
    line_items = draft.get("lineItems") or {}
    edges = line_items.get("edges") or []

    ids = set()
    for edge in edges:
        node = edge.get("node") or {}
        variant = node.get("variant")
        if not variant:
            continue

        inventory_item = variant.get("inventoryItem")
        if not inventory_item:
            continue

        inventory_item_id = inventory_item.get("id")
        tracked = bool(inventory_item.get("tracked"))

        if inventory_item_id and tracked:
            ids.add(inventory_item_id)

    return sorted(ids)


def location_ids_match(returned_location_id: str, configured_location_id: str) -> bool:
    """
    Shopify GraphQL returns location IDs as GIDs, e.g. gid://shopify/Location/123.
    Some env/secrets may be configured as either the full GID or just the numeric ID.
    Compare both safely so inventory lookup does not fail open because of ID format.
    """
    returned_location_id = (returned_location_id or "").strip()
    configured_location_id = (configured_location_id or "").strip()

    if not returned_location_id or not configured_location_id:
        return False

    if returned_location_id == configured_location_id:
        return True

    returned_numeric = returned_location_id.rsplit("/", 1)[-1]
    configured_numeric = configured_location_id.rsplit("/", 1)[-1]

    return returned_numeric == configured_numeric


def available_at_location(inventory_levels_edges: List[dict], location_id: str) -> Optional[int]:
    for edge in inventory_levels_edges:
        node = edge.get("node") or {}
        location = node.get("location") or {}
        returned_location_id = location.get("id") or ""

        if location_ids_match(returned_location_id, location_id):
            for qty in node.get("quantities", []):
                if qty.get("name") == "available":
                    return int(qty.get("quantity", 0))
            return 0

    return None


def fetch_inventory_availability(inventory_item_ids: List[str]) -> Dict[str, Dict[str, Optional[int]]]:
    results: Dict[str, Dict[str, Optional[int]]] = {}

    if not inventory_item_ids:
        return results

    batches = chunked(inventory_item_ids, INVENTORY_BATCH_SIZE)

    for batch_num, batch_ids in enumerate(batches, start=1):
        data = shopify_graphql(
            INVENTORY_ITEMS_QUERY,
            {
                "ids": batch_ids,
                "levelsPageSize": INVENTORY_LEVELS_PAGE_SIZE,
            },
        )

        nodes = data.get("nodes", [])
        logger.info(
            "Fetched inventory threshold batch %s/%s (%s inventory items)",
            batch_num,
            len(batches),
            len(batch_ids),
        )

        for node in nodes:
            if not node:
                continue

            inventory_item_id = node.get("id")
            if not inventory_item_id:
                continue

            levels = node.get("inventoryLevels", {}).get("edges", [])
            results[inventory_item_id] = {
                "sku": node.get("sku") or "(no sku)",
                "available": available_at_location(levels, SHOPIFY_LOCATION_ID),
            }

    return results


def line_inventory_review_label(line: dict, inventory_item: Optional[dict] = None) -> str:
    if inventory_item:
        return (
            inventory_item.get("sku")
            or line.get("sku")
            or (line.get("variant") or {}).get("displayName")
            or line.get("title")
            or "(no sku)"
        )
    return line.get("sku") or line.get("title") or "(no sku)"


def inventory_requirement_lines(draft: dict) -> Tuple[List[dict], List[str]]:
    """
    Returns tracked inventory lines that must be allocated, plus hard-stop
    reasons for anything inventory-related that cannot be safely verified.
    """
    requirement_lines: List[dict] = []
    hard_reasons: List[str] = []

    line_items = draft.get("lineItems") or {}
    edges = line_items.get("edges") or []

    for edge in edges:
        line = edge.get("node") or {}
        title = (line.get("title") or "unknown").strip()
        quantity = int(line.get("quantity") or 0)
        variant = line.get("variant")

        if quantity <= 0:
            continue

        # Custom lines cannot be verified against Shopify inventory.
        if not variant:
            hard_reasons.append(
                f"Custom line item with no variant — cannot verify inventory: {title}"
            )
            continue

        inventory_item = variant.get("inventoryItem")
        if not inventory_item:
            hard_reasons.append(
                f"Variant has no inventory item: {variant.get('displayName', title)}"
            )
            continue

        tracked = bool(inventory_item.get("tracked"))
        sku = line_inventory_review_label(line, inventory_item)
        inventory_item_id = inventory_item.get("id")

        if not tracked:
            logger.info("Draft %s | inventory check ignoring untracked item: %s", draft.get("name"), sku)
            continue

        if not inventory_item_id:
            hard_reasons.append(f"Missing inventory item ID for {sku}")
            continue

        requirement_lines.append(
            {
                "inventory_item_id": inventory_item_id,
                "sku": sku,
                "title": title,
                "quantity": quantity,
            }
        )

    return requirement_lines, hard_reasons


def ensure_inventory_pool_entries(inventory_item_ids: List[str], inventory_pool: InventoryPool) -> None:
    missing_ids = [item_id for item_id in inventory_item_ids if item_id not in inventory_pool]
    if not missing_ids:
        return

    availability_map = fetch_inventory_availability(missing_ids)
    for inventory_item_id in missing_ids:
        info = availability_map.get(inventory_item_id)
        if info:
            inventory_pool[inventory_item_id] = {
                "sku": info.get("sku") or "(no sku)",
                "available": info.get("available"),
            }
        else:
            inventory_pool[inventory_item_id] = {
                "sku": "(unknown sku)",
                "available": None,
            }


def inventory_allocation_review_reasons(
    draft: dict,
    inventory_pool: InventoryPool,
) -> Tuple[List[str], List[str]]:
    """
    Returns (hard_stop_reasons, low_supply_reasons).

    Hard stop means the draft cannot be safely pushed because ordered quantity
    exceeds remaining available inventory, or inventory cannot be verified.

    Low supply means the draft can be filled from current remaining available
    inventory, but doing so would leave less than INVENTORY_REVIEW_THRESHOLD.
    """
    requirement_lines, hard_reasons = inventory_requirement_lines(draft)

    inventory_item_ids = sorted({line["inventory_item_id"] for line in requirement_lines})
    ensure_inventory_pool_entries(inventory_item_ids, inventory_pool)

    low_supply_reasons: List[str] = []

    # Aggregate duplicate SKU / inventory item quantities on the same draft.
    aggregated: Dict[str, dict] = {}
    for line in requirement_lines:
        inventory_item_id = line["inventory_item_id"]
        if inventory_item_id not in aggregated:
            aggregated[inventory_item_id] = {
                "sku": line["sku"],
                "quantity": 0,
            }
        aggregated[inventory_item_id]["quantity"] += int(line["quantity"] or 0)

    for inventory_item_id, requirement in aggregated.items():
        sku = requirement["sku"]
        ordered_quantity = int(requirement["quantity"] or 0)
        pool_info = inventory_pool.get(inventory_item_id) or {}
        available = pool_info.get("available")

        if available is None:
            hard_reasons.append(
                f"No inventory record found at configured location {SHOPIFY_LOCATION_ID} for {sku}"
            )
            continue

        available = int(available)

        if available < ordered_quantity:
            hard_reasons.append(
                f"{sku}: ord {ordered_quantity} | avail {available} | short {ordered_quantity - available}"
            )
            continue

        remaining_after_order = available - ordered_quantity
        if remaining_after_order < INVENTORY_REVIEW_THRESHOLD:
            low_supply_reasons.append(
                f"{sku}: ord {ordered_quantity} | avail {available} | rem {remaining_after_order} | min {INVENTORY_REVIEW_THRESHOLD}"
            )

    return hard_reasons, low_supply_reasons


def reserve_inventory_for_draft(draft: dict, inventory_pool: InventoryPool) -> None:
    """
    Reserve inventory in memory after a draft passes inventory checks. This is
    the guardrail that prevents two drafts in the same run from both consuming
    the same available units.
    """
    requirement_lines, hard_reasons = inventory_requirement_lines(draft)
    if hard_reasons:
        raise RuntimeError(
            "Cannot reserve inventory because the draft has unverifiable inventory lines: "
            + "; ".join(hard_reasons)
        )

    inventory_item_ids = sorted({line["inventory_item_id"] for line in requirement_lines})
    ensure_inventory_pool_entries(inventory_item_ids, inventory_pool)

    aggregated: Dict[str, dict] = {}
    for line in requirement_lines:
        inventory_item_id = line["inventory_item_id"]
        if inventory_item_id not in aggregated:
            aggregated[inventory_item_id] = {
                "sku": line["sku"],
                "quantity": 0,
            }
        aggregated[inventory_item_id]["quantity"] += int(line["quantity"] or 0)

    for inventory_item_id, requirement in aggregated.items():
        sku = requirement["sku"]
        quantity = int(requirement["quantity"] or 0)
        pool_info = inventory_pool.get(inventory_item_id) or {}
        available = pool_info.get("available")

        if available is None or int(available) < quantity:
            raise RuntimeError(
                f"Cannot reserve inventory for {sku}: ordered {quantity}, remaining available {available}"
            )

        inventory_pool[inventory_item_id]["available"] = int(available) - quantity
        logger.info(
            "Draft %s | reserved %s unit(s) of %s in run inventory pool; remaining pool quantity %s",
            draft.get("name"),
            quantity,
            sku,
            inventory_pool[inventory_item_id]["available"],
        )

def ensure_csv_exists() -> None:
    path = Path(CSV_LOG_PATH)
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
            writer.writeheader()


def append_csv_log(row: dict) -> None:
    ensure_csv_exists()
    with Path(CSV_LOG_PATH).open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writerow({key: row.get(key, "") for key in CSV_HEADERS})


def current_shipping_title(draft: dict) -> str:
    shipping_line = draft.get("shippingLine") or {}
    return (shipping_line.get("title") or "").strip()


def current_shipping_price(draft: dict) -> str:
    shipping_line = draft.get("shippingLine") or {}
    discounted = shipping_line.get("discountedPriceSet") or {}
    shop_money = discounted.get("shopMoney") or {}
    return (shop_money.get("amount") or "").strip()


def log_draft_result(
    draft: dict,
    *,
    action: str,
    success: bool,
    reason: str = "",
    detected_terms: str = "",
    existing_terms_before: str = "",
    payment_terms_after: str = "",
    freight_action: str = "",
    freight_title: str = "",
    freight_price: str = "",
    order_id: str = "",
    order_name: str = "",
) -> None:
    ship_date_value = ""
    if draft.get("metafield"):
        ship_date_value = draft["metafield"].get("value") or ""

    order = draft.get("order") or {}
    order_tags = normalize_tags(order.get("tags", []))

    row = {
        "pushed_at": datetime.now(timezone.utc).isoformat(),
        "dry_run": DRY_RUN,
        "draft_id": draft.get("id", ""),
        "draft_name": draft.get("name", ""),
        "po_number": draft.get("poNumber", ""),
        "company": safe_company_name(draft),
        "status": draft.get("status", ""),
        "action": action,
        "success": success,
        "reason": reason,
        "detected_terms": detected_terms,
        "existing_terms_before": existing_terms_before,
        "payment_terms_after": payment_terms_after,
        "ship_date": ship_date_value,
        "freight_action": freight_action,
        "freight_title": freight_title or current_shipping_title(draft),
        "freight_price": freight_price or current_shipping_price(draft),
        "final_tags": ",".join(order_tags if order_tags else normalize_tags(draft.get("tags", []))),
        "order_id": order_id or order.get("id", ""),
        "order_name": order_name or order.get("name", ""),
    }
    append_csv_log(row)


def fetch_candidate_drafts() -> List[dict]:
    drafts: List[dict] = []
    cursor = None

    while True:
        data = shopify_graphql(
            CANDIDATE_DRAFTS_QUERY,
            {
                "cursor": cursor,
                "pageSize": DRAFTS_PAGE_SIZE,
                "shipDateNamespace": SHIP_DATE_NAMESPACE,
                "shipDateKey": SHIP_DATE_KEY,
            },
        )
        connection = data["draftOrders"]
        drafts.extend(edge["node"] for edge in connection["edges"])

        if not connection["pageInfo"]["hasNextPage"]:
            break

        cursor = connection["pageInfo"]["endCursor"]

    logger.info("Fetched %s candidate draft(s)", len(drafts))
    if PROCESS_ONLY_DRAFTS:
        logger.info("PROCESS_ONLY_DRAFTS active — hard allowlist: %s", sorted(PROCESS_ONLY_DRAFTS))
    if COMPLETE_DRAFT_NAMES:
        logger.info("COMPLETE_DRAFT_NAMES active: %s", sorted(COMPLETE_DRAFT_NAMES))
    if EXCLUDED_SKUS:
        logger.info("EXCLUDED_SKUS active: %s", sorted(EXCLUDED_SKUS))
    return drafts


def lookup_variant_titles_by_sku(skus: List[str]) -> Dict[str, str]:
    """
    Looks up a human-readable "Product Title - Variant" label for each SKU,
    one Shopify query per SKU (the list is small and only runs once per
    script execution, so this does not need batching).

    Read-only — does not touch draft orders, inventory, or anything else
    process_draft() depends on. A SKU that errors or has no match simply
    falls back to the bare SKU string as its own label, so a single bad
    lookup never breaks the snapshot for the rest of the list.
    """
    titles: Dict[str, str] = {}

    for sku in sorted(skus):
        try:
            data = shopify_graphql(
                VARIANT_BY_SKU_QUERY,
                {"query": f"sku:{sku}"},
            )
            edges = (data.get("productVariants") or {}).get("edges") or []

            match = None
            for edge in edges:
                node = edge.get("node") or {}
                if (node.get("sku") or "").strip().upper() == sku.upper():
                    match = node
                    break

            if match:
                product_title = ((match.get("product") or {}).get("title") or "").strip()
                variant_name = (match.get("displayName") or "").strip()
                if product_title and variant_name and variant_name.upper() != "DEFAULT TITLE":
                    titles[sku] = f"{product_title} - {variant_name}"
                elif product_title:
                    titles[sku] = product_title
                else:
                    titles[sku] = sku
            else:
                titles[sku] = sku
                logger.warning("Excluded SKU snapshot: no product match found for %s", sku)
        except Exception as exc:
            titles[sku] = sku
            logger.warning("Excluded SKU snapshot: lookup failed for %s: %s", sku, exc)

    return titles


def publish_excluded_skus_snapshot() -> None:
    """
    Writes a JSON snapshot of the current EXCLUDED_SKUS list, with product
    titles attached, to EXCLUDED_SKUS_EXPORT_PATH for the Ops Scorecard
    dashboard to read (e.g. via a committed file in CI).

    This is purely additive reporting: it is read-only against Shopify,
    never reads back its own output, and is wrapped in its own try/except
    by the caller so a failure here can never affect order processing.
    """
    skus = sorted(EXCLUDED_SKUS)
    titles = lookup_variant_titles_by_sku(skus) if skus else {}

    snapshot = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "count": len(skus),
        "skus": [
            {"sku": sku, "title": titles.get(sku, sku)}
            for sku in skus
        ],
    }

    path = Path(EXCLUDED_SKUS_EXPORT_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2)

    logger.info(
        "Published excluded SKU snapshot to %s (%s SKU(s))",
        EXCLUDED_SKUS_EXPORT_PATH,
        len(skus),
    )


def recheck_draft(draft_id: str) -> dict:
    data = shopify_graphql(
        DRAFT_RECHECK_QUERY,
        {
            "id": draft_id,
            "shipDateNamespace": SHIP_DATE_NAMESPACE,
            "shipDateKey": SHIP_DATE_KEY,
        },
    )
    draft = data.get("draftOrder")
    if not draft:
        raise RuntimeError(f"Draft {draft_id} not found during recheck")
    return draft


def recheck_order(order_id: str) -> dict:
    data = shopify_graphql(ORDER_RECHECK_QUERY, {"id": order_id})
    order = data.get("order")
    if not order:
        raise RuntimeError(f"Order {order_id} not found during recheck")
    return order


def update_draft(draft_id: str, input_payload: dict) -> dict:
    if DRY_RUN:
        logger.info("DRY RUN | would update draft %s with %s", draft_id, input_payload)
        return {}

    data = shopify_graphql(
        DRAFT_UPDATE_MUTATION,
        {"id": draft_id, "input": input_payload},
    )
    user_errors = data["draftOrderUpdate"].get("userErrors", [])
    if user_errors:
        raise RuntimeError(f"draftOrderUpdate userErrors: {user_errors}")
    return data["draftOrderUpdate"]["draftOrder"]


def update_order_tags(order_id: str, final_tags: List[str]) -> dict:
    if DRY_RUN:
        logger.info("DRY RUN | would update order %s tags to %s", order_id, final_tags)
        return {}

    data = shopify_graphql(
        ORDER_UPDATE_MUTATION,
        {
            "input": {
                "id": order_id,
                "tags": normalize_tags(final_tags),
            }
        },
    )
    user_errors = data["orderUpdate"].get("userErrors", [])
    if user_errors:
        raise RuntimeError(f"orderUpdate userErrors: {user_errors}")
    return data["orderUpdate"]["order"]


def set_draft_inventory_review_metafield(draft_id: str, value: str) -> dict:
    """
    Stores human-readable inventory review details directly on the draft order.
    Create a Shopify draft order metafield definition with:
      namespace: b2b
      key: inventory_review_reason
      type: Multi-line text
    or override these with INVENTORY_REVIEW_NAMESPACE / KEY / TYPE.
    """
    value = (value or "").strip()
    if not value:
        return {}

    metafield_payload = {
        "ownerId": draft_id,
        "namespace": INVENTORY_REVIEW_NAMESPACE,
        "key": INVENTORY_REVIEW_KEY,
        "type": INVENTORY_REVIEW_TYPE,
        "value": value,
    }

    if DRY_RUN:
        logger.info("DRY RUN | would set draft inventory review metafield on %s: %s", draft_id, metafield_payload)
        return {}

    data = shopify_graphql(
        METAFIELDS_SET_MUTATION,
        {"metafields": [metafield_payload]},
    )
    user_errors = data["metafieldsSet"].get("userErrors", [])
    if user_errors:
        raise RuntimeError(f"metafieldsSet userErrors: {user_errors}")
    metafields = data["metafieldsSet"].get("metafields") or []
    return metafields[0] if metafields else {}


def complete_draft(draft_id: str, is_free: bool = False) -> dict:
    if DRY_RUN:
        logger.info("DRY RUN | would complete draft %s (is_free=%s)", draft_id, is_free)
        return {}

    if is_free:
        data = shopify_graphql(DRAFT_COMPLETE_MUTATION_FREE, {"id": draft_id})
    else:
        data = shopify_graphql(
            DRAFT_COMPLETE_MUTATION,
            {"id": draft_id, "paymentPending": True},
        )

    user_errors = data["draftOrderComplete"].get("userErrors", [])
    if user_errors:
        raise RuntimeError(f"draftOrderComplete userErrors: {user_errors}")
    return data["draftOrderComplete"]["draftOrder"]


def strip_payment_terms(draft_id: str, name: str) -> bool:
    if DRY_RUN:
        logger.info("DRY RUN | would strip payment terms from %s", name)
        return True

    logger.info("%s | stripping payment terms before free order completion", name)
    try:
        update_draft(draft_id, {"paymentTerms": None})
    except Exception as exc:
        logger.warning("%s | payment terms strip attempt raised: %s", name, exc)

    latest = recheck_draft(draft_id)
    if latest.get("paymentTerms"):
        logger.warning(
            "%s | payment terms still present after strip attempt: %s",
            name,
            payment_terms_name(latest.get("paymentTerms")),
        )
        return False

    logger.info("%s | payment terms successfully stripped", name)
    return True


def claim_draft(draft: dict) -> None:
    current_tags = normalize_tags(draft.get("tags", []))
    claimed_tags = add_tags(current_tags, PROCESSING_TAG)
    update_draft(draft["id"], {"tags": claimed_tags})


def release_claim(draft: dict) -> None:
    current_tags = normalize_tags(draft.get("tags", []))
    released_tags = remove_tags(current_tags, PROCESSING_TAG)
    update_draft(draft["id"], {"tags": released_tags})


def clear_submitted_tag(draft: dict) -> None:
    current_tags = normalize_tags(draft.get("tags", []))
    cleaned_tags = remove_tags(current_tags, SUBMITTED_TAG)
    update_draft(draft["id"], {"tags": cleaned_tags})


def mark_needs_review(draft: dict, reason: Optional[str] = None) -> None:
    current_tags = normalize_tags(draft.get("tags", []))
    final_tags = add_tags(current_tags, NEEDS_REVIEW_TAG)
    final_tags = remove_tags(
        final_tags,
        PROCESSING_TAG,
        SUBMITTED_TAG,
    )
    update_draft(draft["id"], {"tags": final_tags})
    if reason:
        logger.warning("%s | marked %s | %s", draft.get("name"), NEEDS_REVIEW_TAG, reason)


def mark_inventory_shortage(draft: dict, reason: Optional[str] = None) -> None:
    current_tags = normalize_tags(draft.get("tags", []))
    final_tags = add_tags(current_tags, NEEDS_REVIEW_TAG, INVENTORY_SHORTAGE_TAG)
    final_tags = remove_tags(
        final_tags,
        PROCESSING_TAG,
        SUBMITTED_TAG,
    )
    update_draft(draft["id"], {"tags": final_tags})
    if reason:
        set_draft_inventory_review_metafield(draft["id"], reason)
        logger.warning("%s | marked %s/%s | %s", draft.get("name"), NEEDS_REVIEW_TAG, INVENTORY_SHORTAGE_TAG, reason)


def mark_low_supply(draft: dict, reason: Optional[str] = None) -> None:
    current_tags = normalize_tags(draft.get("tags", []))
    final_tags = add_tags(current_tags, LOW_SUPPLY_TAG)
    final_tags = remove_tags(
        final_tags,
        PROCESSING_TAG,
        SUBMITTED_TAG,
    )
    update_draft(draft["id"], {"tags": final_tags})
    if reason:
        set_draft_inventory_review_metafield(draft["id"], reason)
        logger.warning("%s | marked %s | %s", draft.get("name"), LOW_SUPPLY_TAG, reason)


def validate_completion_result(
    *,
    name: str,
    draft_after_complete: dict,
    completed_payload: dict,
) -> Tuple[str, str]:
    payload_order = (completed_payload or {}).get("order") or {}
    latest_order = (draft_after_complete.get("order") or {})

    order_id = latest_order.get("id") or payload_order.get("id") or ""
    order_name = latest_order.get("name") or payload_order.get("name") or ""

    if draft_after_complete.get("status") == "OPEN":
        raise RuntimeError(
            f"{name} completion mutation returned but draft is still OPEN after recheck"
        )

    if not order_id:
        raise RuntimeError(
            f"{name} completion mutation returned but no Shopify order was attached after recheck"
        )

    return order_id, order_name


def finalize_completed_order_tags(
    *,
    draft_before_complete: dict,
    order_id: str,
    order_name: str,
) -> dict:
    draft_tags_before_complete = normalize_tags(draft_before_complete.get("tags", []))
    final_order_tags = add_tags(
        remove_tags(draft_tags_before_complete, PROCESSING_TAG, NEEDS_REVIEW_TAG, LOW_SUPPLY_TAG, INVENTORY_SHORTAGE_TAG),
        SUBMITTED_TAG,
    )

    logger.info(
        "%s | updating completed order tags to remove %s and add %s",
        order_name or order_id,
        PROCESSING_TAG,
        SUBMITTED_TAG,
    )
    update_order_tags(order_id, final_order_tags)
    latest_order = recheck_order(order_id)

    latest_order_tags = normalize_tags(latest_order.get("tags", []))
    if PROCESSING_TAG in latest_order_tags:
        raise RuntimeError(
            f"{order_name or order_id} completed but {PROCESSING_TAG} is still present on order after tag update"
        )
    if SUBMITTED_TAG not in latest_order_tags:
        raise RuntimeError(
            f"{order_name or order_id} completed but {SUBMITTED_TAG} is missing on order after tag update"
        )

    try:
        logger.info(
            "%s | cleaning completed draft tags to remove %s and add %s",
            draft_before_complete.get("name") or draft_before_complete.get("id"),
            PROCESSING_TAG,
            SUBMITTED_TAG,
        )
        update_draft(draft_before_complete["id"], {"tags": final_order_tags})
    except Exception as exc:
        logger.warning(
            "%s | completed order tags were updated, but completed draft tag cleanup failed: %s",
            draft_before_complete.get("name") or draft_before_complete.get("id"),
            exc,
        )

    return latest_order


def parse_ship_date(raw_value: Optional[str]) -> Optional[date]:
    if not raw_value:
        return None

    raw = raw_value.strip()
    if not raw:
        return None

    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            pass

    try:
        normalized = raw.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized).date()
    except ValueError:
        return None


def ship_date_allows_release(raw_value: Optional[str], today: date) -> Tuple[bool, str]:
    parsed = parse_ship_date(raw_value)

    if parsed is None:
        return True, "No ship date found"

    days_until = (parsed - today).days
    if days_until <= 7:
        return True, f"Ship date {parsed.isoformat()} is within {days_until} day(s)"
    return False, f"Ship date {parsed.isoformat()} is more than 7 days out"


def build_note_blob(draft: dict) -> str:
    parts = [
        draft.get("note2") or "",
        draft.get("poNumber") or "",
    ]
    return "\n".join(parts).strip()


def normalize_terms_text(text: str) -> str:
    upper = (text or "").upper()
    upper = upper.replace("TERMS:", " ")
    upper = upper.replace("TERMS", " ")
    return re.sub(r"[^A-Z0-9]+", "", upper)


def detect_net_terms_days(text: str) -> Optional[int]:
    if not text:
        return None

    normalized = normalize_terms_text(text)

    checks = [
        (120, ["NET120", "N120"]),
        (90, ["NET90", "N90"]),
        (60, ["NET60", "N60"]),
        (45, ["NET45", "N45"]),
        (30, ["NET30", "N30"]),
    ]
    for days, tokens in checks:
        for token in tokens:
            if token in normalized:
                return days

    haystack = text.upper()
    patterns = {
        120: [r"\bNET[\s\-_\/:]*120\b", r"\bN[\s\-_\/:]*120\b"],
        90: [r"\bNET[\s\-_\/:]*90\b", r"\bN[\s\-_\/:]*90\b"],
        60: [r"\bNET[\s\-_\/:]*60\b", r"\bN[\s\-_\/:]*60\b"],
        45: [r"\bNET[\s\-_\/:]*45\b", r"\bN[\s\-_\/:]*45\b"],
        30: [r"\bNET[\s\-_\/:]*30\b", r"\bN[\s\-_\/:]*30\b"],
    }
    for days, regexes in patterns.items():
        for pattern in regexes:
            if re.search(pattern, haystack, flags=re.IGNORECASE):
                return days

    return None


def parse_decimal(value: Optional[str]) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None


def money_to_str(amount: Decimal) -> str:
    return format(amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP), "f")


def draft_subtotal_amount(draft: dict) -> Optional[Decimal]:
    subtotal_set = draft.get("subtotalPriceSet") or {}
    shop_money = subtotal_set.get("shopMoney") or {}
    return parse_decimal(shop_money.get("amount"))


def valid_free_freight_marker_present(text: str) -> bool:
    if not text:
        return False
    patterns = [
        r"\bFF\b",
        r"\bFFA\b",
        r"\bF\s*/\s*F\b",
        r"\bFREE\s+FREIGHT\b",
        r"\bFREIGHT\s+FREE\b",
        r"\bFEDEXA\b",
        r"\bSHIP\s+(?:FED\s*EX|FEDEX|UPS|DHL|USPS)\s+\w+(?:\s+\w+)?\s+ACCOUNT\s+\d+",
    ]
    return any(re.search(p, text, flags=re.IGNORECASE) for p in patterns)


def detect_freight_title(text: str) -> str:
    if not text:
        return DEFAULT_FREIGHT_TITLE

    if re.search(r"\bUPS\s+GROUND\b", text, flags=re.IGNORECASE):
        return "UPS Ground"
    if re.search(r"\b(?:SHIP\s+)?UPS\b", text, flags=re.IGNORECASE):
        return "UPS"
    if re.search(r"\b(?:SHIP\s+)?FED\s*EX\b", text, flags=re.IGNORECASE):
        return "FedEx"
    if re.search(r"\b(?:SHIP\s+)?FEDEX\b", text, flags=re.IGNORECASE):
        return "FedEx"

    return DEFAULT_FREIGHT_TITLE


def build_freight_quote(draft: dict) -> Tuple[bool, str, str, str]:
    blob = build_note_blob(draft)

    if valid_free_freight_marker_present(blob):
        return True, "free-freight", "", ""

    subtotal = draft_subtotal_amount(draft)
    if subtotal is None:
        return False, "Could not determine draft subtotal for freight calculation", "", ""

    freight_title = detect_freight_title(blob)
    freight_amount = (subtotal * FREIGHT_RATE_PERCENT / Decimal("100")).quantize(
        Decimal("0.01"),
        rounding=ROUND_HALF_UP,
    )
    return True, "charge-freight", freight_title, money_to_str(freight_amount)


def shipping_line_matches(draft: dict, expected_title: str, expected_price: str) -> bool:
    shipping_line = draft.get("shippingLine") or {}
    if not shipping_line:
        return False

    current_title = (shipping_line.get("title") or "").strip()
    current_price = parse_decimal(current_shipping_price(draft))
    desired_price = parse_decimal(expected_price)

    if current_title != expected_title:
        return False
    if current_price is None or desired_price is None:
        return False
    return current_price == desired_price


def ensure_shipping_logic(draft: dict) -> Tuple[bool, str, str, str, str]:
    ok, freight_action, freight_title, freight_price = build_freight_quote(draft)
    if not ok:
        return False, freight_action, freight_title, freight_price, freight_action

    if freight_action == "free-freight":
        return True, freight_action, freight_title, freight_price, "Valid free-freight marker found; leaving shipping unchanged"

    if shipping_line_matches(draft, freight_title, freight_price):
        return True, freight_action, freight_title, freight_price, (
            f"Existing shipping already matches {freight_title} at {freight_price}"
        )

    currency_code = (draft.get("currencyCode") or "").strip() or "USD"
    shipping_payload = {
        "shippingLine": {
            "title": freight_title,
            "priceWithCurrency": {
                "amount": freight_price,
                "currencyCode": currency_code,
            },
        }
    }

    logger.info(
        "Draft %s | setting custom shipping to %s at %s %s",
        draft["name"],
        freight_title,
        freight_price,
        currency_code,
    )
    update_draft(draft["id"], shipping_payload)
    return True, freight_action, freight_title, freight_price, (
        f"Set shipping to {freight_title} at {freight_price} {currency_code}"
    )


def build_issued_at(now_dt: datetime) -> str:
    return now_dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def build_due_at(days_from_today: int, today: date) -> str:
    due_date = today + timedelta(days=days_from_today)
    due_at = datetime.combine(due_date, time(0, 0, 0), tzinfo=timezone.utc)
    return due_at.isoformat().replace("+00:00", "Z")


def payment_terms_match_detected(payment_terms: Optional[dict], detected_days: Optional[int]) -> bool:
    if not payment_terms or not detected_days:
        return False

    name = (payment_terms_name(payment_terms) or "").strip().upper()
    due_in_days = payment_terms.get("dueInDays")

    if detected_days in (30, 45, 60, 90):
        if due_in_days == detected_days:
            return True
        if f"NET {detected_days}" in name or f"NET{detected_days}" in name:
            return True
        return False

    if detected_days == 120:
        if "NET 120" in name or "NET120" in name:
            return True
        if "FIXED" in name:
            return True
        if due_in_days == 120:
            return True
        return False

    return False


def is_issue_date_fixed_terms_error(exc: Exception) -> bool:
    return "issue date cannot be set with event or fixed payment terms" in str(exc).lower()


def try_update_payment_terms_payloads(
    draft: dict,
    payloads: List[Tuple[dict, str]],
) -> Tuple[bool, str]:
    last_exc: Optional[Exception] = None

    for payload, description in payloads:
        try:
            logger.info("Draft %s | attempting payment terms update: %s", draft["name"], description)
            update_draft(draft["id"], payload)
            return True, description
        except Exception as exc:
            last_exc = exc
            logger.warning(
                "Draft %s | payment terms update attempt failed (%s): %s",
                draft["name"],
                description,
                exc,
            )

            if is_issue_date_fixed_terms_error(exc):
                continue
            raise

    if last_exc:
        raise last_exc

    return False, "No payment terms payloads were attempted"


def ensure_payment_terms(draft: dict, now_dt: datetime) -> Tuple[bool, str, str, Optional[int]]:
    existing = draft.get("paymentTerms")
    existing_name = payment_terms_name(existing)

    blob = build_note_blob(draft)
    detected_days = detect_net_terms_days(blob)

    if detected_days:
        detected_label = f"Net {detected_days}"
    else:
        detected_days = 30
        detected_label = "Net 30 (defaulted)"

    template_id = PAYMENT_TEMPLATE_MAP.get(detected_days, "").strip()
    if not template_id:
        logger.warning(
            "Draft %s | detected Net %s but no template ID configured; defaulting to Net 30 template %s",
            draft["name"],
            detected_days,
            DEFAULT_PAYMENT_TERMS_TEMPLATE_ID,
        )
        detected_days = 30
        detected_label = "Net 30 (defaulted)"
        template_id = DEFAULT_PAYMENT_TERMS_TEMPLATE_ID

    issued_at = build_issued_at(now_dt)

    if detected_days == 120:
        due_at = build_due_at(120, now_dt.date())

        payloads = [
            (
                {
                    "paymentTerms": {
                        "paymentTermsTemplateId": template_id,
                        "paymentSchedules": [
                            {
                                "dueAt": due_at,
                            }
                        ],
                    }
                },
                f"fixed/event-safe update to Net 120 using template {template_id} with dueAt {due_at}",
            ),
            (
                {
                    "paymentTerms": {
                        "paymentTermsTemplateId": template_id,
                    }
                },
                f"template-only fallback to Net 120 using template {template_id}",
            ),
        ]

        logger.info(
            "Draft %s | overriding existing payment terms '%s' from note/PO to Net 120 using fixed-safe logic and template %s",
            draft["name"],
            existing_name or "NONE",
            template_id,
        )

        ok, attempt_description = try_update_payment_terms_payloads(draft, payloads)
        if not ok:
            return False, "Failed to update payment terms to Net 120", detected_label, detected_days

        return True, f"Overrode payment terms to Net 120 ({attempt_description})", detected_label, detected_days

    payloads = [
        (
            {
                "paymentTerms": {
                    "paymentTermsTemplateId": template_id,
                    "paymentSchedules": [
                        {
                            "issuedAt": issued_at,
                        }
                    ],
                }
            },
            f"standard update to Net {detected_days} using template {template_id} with issuedAt {issued_at}",
        ),
        (
            {
                "paymentTerms": {
                    "paymentTermsTemplateId": template_id,
                }
            },
            f"template-only fallback to Net {detected_days} using template {template_id}",
        ),
    ]

    logger.info(
        "Draft %s | overriding existing payment terms '%s' to %s using template %s",
        draft["name"],
        existing_name or "NONE",
        detected_label,
        template_id,
    )

    ok, attempt_description = try_update_payment_terms_payloads(draft, payloads)
    if not ok:
        return False, f"Failed to update payment terms to {detected_label}", detected_label, detected_days

    return True, f"Overrode payment terms to {detected_label} ({attempt_description})", detected_label, detected_days


def process_draft(draft: dict, now_dt: datetime, inventory_pool: InventoryPool) -> None:
    today = now_dt.date()
    name = draft["name"]
    draft_id = draft["id"]
    tags = normalize_tags(draft.get("tags", []))
    existing_terms_before = payment_terms_name(draft.get("paymentTerms"))
    freight_action = ""
    freight_title = ""
    freight_price = ""

    if not should_process_draft(name):
        logger.info("Skipping %s because it is not in the active allowlist", name)
        return

    if has_excluded_tag(tags):
        logger.info("Skipping %s because it has excluded tags", name)
        return

    if should_exclude_customer(draft):
        logger.info("Skipping %s because customer is excluded: %s", name, safe_company_name(draft))
        return

    excluded_skus = excluded_skus_on_draft(draft)
    if excluded_skus:
        logger.info("Skipping %s because it contains excluded SKU(s): %s", name, ", ".join(excluded_skus))
        log_draft_result(
            draft,
            action="skipped",
            success=False,
            reason=f"Excluded SKU(s): {', '.join(excluded_skus)}",
            existing_terms_before=existing_terms_before,
        )
        return

    logger.info("-----")
    logger.info("Evaluating %s", name)

    claim_draft(draft)

    latest = recheck_draft(draft_id)
    latest_tags = normalize_tags(latest.get("tags", []))

    if latest.get("order"):
        order = latest.get("order") or {}
        order_id = order.get("id", "")
        order_name = order.get("name", "")
        logger.info("%s already has an order; updating order tags", name)

        finalize_completed_order_tags(
            draft_before_complete=latest,
            order_id=order_id,
            order_name=order_name,
        )

        latest = recheck_draft(draft_id)
        log_draft_result(
            latest,
            action="already-submitted",
            success=True,
            reason="Draft already had an order",
            existing_terms_before=existing_terms_before,
            payment_terms_after=payment_terms_name(latest.get("paymentTerms")),
            order_id=order_id,
            order_name=order_name,
        )
        return

    if latest.get("status") != "OPEN":
        logger.info("%s is no longer OPEN; releasing claim", name)
        release_claim(latest)
        latest = recheck_draft(draft_id)
        log_draft_result(
            latest,
            action="released-claim",
            success=False,
            reason="Draft no longer open",
            existing_terms_before=existing_terms_before,
            payment_terms_after=payment_terms_name(latest.get("paymentTerms")),
        )
        return

    if READY_TAG not in latest_tags:
        logger.info("%s no longer has %s; releasing claim", name, READY_TAG)
        release_claim(latest)
        latest = recheck_draft(draft_id)
        log_draft_result(
            latest,
            action="released-claim",
            success=False,
            reason=f"Draft no longer tagged {READY_TAG}",
            existing_terms_before=existing_terms_before,
            payment_terms_after=payment_terms_name(latest.get("paymentTerms")),
        )
        return

    if NEEDS_REVIEW_TAG in latest_tags:
        logger.info("%s now has %s; releasing claim", name, NEEDS_REVIEW_TAG)
        release_claim(latest)
        latest = recheck_draft(draft_id)
        log_draft_result(
            latest,
            action="released-claim",
            success=False,
            reason=f"Draft tagged {NEEDS_REVIEW_TAG}",
            existing_terms_before=existing_terms_before,
            payment_terms_after=payment_terms_name(latest.get("paymentTerms")),
        )
        return

    if has_excluded_tag(latest_tags):
        logger.info("%s now has an excluded tag; releasing claim", name)
        release_claim(latest)
        latest = recheck_draft(draft_id)
        log_draft_result(
            latest,
            action="released-claim",
            success=False,
            reason="Draft has excluded tag",
            existing_terms_before=existing_terms_before,
            payment_terms_after=payment_terms_name(latest.get("paymentTerms")),
        )
        return

    if should_exclude_customer(latest):
        logger.info("%s now has excluded customer after recheck: %s", name, safe_company_name(latest))
        release_claim(latest)
        latest = recheck_draft(draft_id)
        log_draft_result(
            latest,
            action="skipped",
            success=False,
            reason=f"Excluded customer: {safe_company_name(latest)}",
            existing_terms_before=existing_terms_before,
            payment_terms_after=payment_terms_name(latest.get("paymentTerms")),
        )
        return

    excluded_skus = excluded_skus_on_draft(latest)
    if excluded_skus:
        logger.info("%s now has excluded SKU(s) after recheck: %s", name, ", ".join(excluded_skus))
        release_claim(latest)
        latest = recheck_draft(draft_id)
        log_draft_result(
            latest,
            action="skipped",
            success=False,
            reason=f"Excluded SKU(s): {', '.join(excluded_skus)}",
            existing_terms_before=existing_terms_before,
            payment_terms_after=payment_terms_name(latest.get("paymentTerms")),
        )
        return

    ship_date_value = ""
    if latest.get("metafield"):
        ship_date_value = latest["metafield"].get("value") or ""

    ship_ok, ship_reason = ship_date_allows_release(ship_date_value, today)
    logger.info("%s | ship-date-check=%s | %s", name, ship_ok, ship_reason)

    if not ship_ok:
        release_claim(latest)
        latest = recheck_draft(draft_id)
        log_draft_result(
            latest,
            action="skipped",
            success=False,
            reason=ship_reason,
            existing_terms_before=existing_terms_before,
            payment_terms_after=payment_terms_name(latest.get("paymentTerms")),
        )
        return

    freight_ok, freight_action, freight_title, freight_price, freight_reason = ensure_shipping_logic(latest)
    logger.info("%s | freight-check=%s | %s", name, freight_ok, freight_reason)

    latest = recheck_draft(draft_id)
    current_freight_title = current_shipping_title(latest)
    current_freight_price = current_shipping_price(latest)
    logger.info(
        "%s | shipping after update: %s @ %s",
        name,
        current_freight_title or "NONE",
        current_freight_price or "NONE",
    )

    if not freight_ok:
        mark_needs_review(latest, freight_reason)
        latest = recheck_draft(draft_id)
        log_draft_result(
            latest,
            action="needs-review",
            success=False,
            reason=freight_reason,
            existing_terms_before=existing_terms_before,
            payment_terms_after=payment_terms_name(latest.get("paymentTerms")),
            freight_action=freight_action,
            freight_title=freight_title,
            freight_price=freight_price,
        )
        return

    if not DRY_RUN and freight_action == "charge-freight":
        if not shipping_line_matches(latest, freight_title, freight_price):
            freight_ok = False
            freight_reason = (
                f"Expected shipping '{freight_title}' at {freight_price} but Shopify returned "
                f"'{current_freight_title or 'NONE'}' at {current_freight_price or 'NONE'}'"
            )
    elif DRY_RUN and freight_action == "charge-freight":
        logger.info(
            "%s | DRY RUN active; shipping remains unchanged on recheck because no mutation was sent",
            name,
        )

    if not freight_ok:
        mark_needs_review(latest, freight_reason)
        latest = recheck_draft(draft_id)
        log_draft_result(
            latest,
            action="needs-review",
            success=False,
            reason=freight_reason,
            existing_terms_before=existing_terms_before,
            payment_terms_after=payment_terms_name(latest.get("paymentTerms")),
            freight_action=freight_action,
            freight_title=freight_title,
            freight_price=freight_price,
        )
        return

    subtotal = draft_subtotal_amount(latest)
    is_free_order = subtotal is not None and subtotal == Decimal("0.00")

    if is_free_order:
        assert subtotal == Decimal("0.00"), (
            f"Safety check failed: expected $0.00 but got {subtotal} on {name}"
        )

        logger.info(
            "%s | $0.00 order detected; attempting to remove payment terms before free completion",
            name,
        )

        terms_stripped = strip_payment_terms(draft_id, name)
        latest = recheck_draft(draft_id)

        if terms_stripped:
            terms_reason = "Skipped - $0 free order; payment terms stripped"
        else:
            terms_reason = (
                "Skipped - $0 free order; payment terms could not be stripped, "
                "will attempt free-order completion anyway"
            )
            logger.warning("%s | %s", name, terms_reason)

        terms_ok = True
        detected_terms = ""
        detected_days = None
    else:
        terms_ok, terms_reason, detected_terms, detected_days = ensure_payment_terms(latest, now_dt)

    logger.info("%s | payment-terms-check=%s | %s", name, terms_ok, terms_reason)

    latest = recheck_draft(draft_id)
    payment_terms_after = payment_terms_name(latest.get("paymentTerms"))
    logger.info(
        "%s | payment terms after update: %s",
        name,
        payment_terms_after or "NONE",
    )

    if not DRY_RUN and detected_terms:
        if not payment_terms_after:
            terms_ok = False
            terms_reason = f"Detected {detected_terms} but payment terms are still blank after update"
        elif not payment_terms_match_detected(latest.get("paymentTerms"), detected_days):
            terms_ok = False
            terms_reason = (
                f"Detected {detected_terms} but Shopify returned "
                f"'{payment_terms_after}' after update"
            )
    elif DRY_RUN and detected_terms and not payment_terms_after:
        logger.info(
            "%s | DRY RUN active; payment terms remain unchanged on recheck because no mutation was sent",
            name,
        )

    if not terms_ok:
        mark_needs_review(latest, terms_reason)
        latest = recheck_draft(draft_id)
        log_draft_result(
            latest,
            action="needs-review",
            success=False,
            reason=terms_reason,
            detected_terms=detected_terms,
            existing_terms_before=existing_terms_before,
            payment_terms_after=payment_terms_name(latest.get("paymentTerms")),
            freight_action=freight_action,
            freight_title=freight_title,
            freight_price=freight_price,
        )
        return

    latest = recheck_draft(draft_id)
    inventory_hard_reasons, inventory_low_supply_reasons = inventory_allocation_review_reasons(latest, inventory_pool)

    if inventory_hard_reasons:
        inventory_reason = "; ".join(inventory_hard_reasons)
        logger.info("%s | inventory-allocation-check=False | %s", name, inventory_reason)

        mark_inventory_shortage(latest, inventory_reason)
        latest = recheck_draft(draft_id)
        log_draft_result(
            latest,
            action="inventory-shortage",
            success=False,
            reason=inventory_reason,
            detected_terms=detected_terms,
            existing_terms_before=existing_terms_before,
            payment_terms_after=payment_terms_name(latest.get("paymentTerms")),
            freight_action=freight_action,
            freight_title=freight_title,
            freight_price=freight_price,
        )
        return

    if inventory_low_supply_reasons:
        inventory_reason = "; ".join(inventory_low_supply_reasons)
        logger.info("%s | inventory-low-supply-check=False | %s", name, inventory_reason)

        mark_low_supply(latest, inventory_reason)
        latest = recheck_draft(draft_id)
        log_draft_result(
            latest,
            action="low-supply",
            success=False,
            reason=inventory_reason,
            detected_terms=detected_terms,
            existing_terms_before=existing_terms_before,
            payment_terms_after=payment_terms_name(latest.get("paymentTerms")),
            freight_action=freight_action,
            freight_title=freight_title,
            freight_price=freight_price,
        )
        return

    logger.info(
        "%s | inventory-allocation-check=True | all tracked items can be filled and remain at or above threshold %s after this draft",
        name,
        INVENTORY_REVIEW_THRESHOLD,
    )

    draft_state_before_complete = recheck_draft(draft_id)
    reserve_inventory_for_draft(draft_state_before_complete, inventory_pool)

    if is_free_order:
        logger.info("%s | $0.00 order; using free-order completion mutation", name)

    completed = complete_draft(draft_id, is_free=is_free_order)

    order_id = ""
    order_name = ""
    latest = recheck_draft(draft_id)

    if not DRY_RUN:
        order_id, order_name = validate_completion_result(
            name=name,
            draft_after_complete=latest,
            completed_payload=completed,
        )
        logger.info(
            "%s | completed successfully -> order %s",
            name,
            order_name or order_id,
        )

        latest_order = finalize_completed_order_tags(
            draft_before_complete=draft_state_before_complete,
            order_id=order_id,
            order_name=order_name,
        )

        logger.info(
            "%s | final order tags: %s",
            order_name or order_id,
            ", ".join(normalize_tags(latest_order.get("tags", []))),
        )

        latest = recheck_draft(draft_id)

    if DRY_RUN:
        log_draft_result(
            latest,
            action="dry-run-complete",
            success=True,
            reason="Draft would have been completed" + (" (free order)" if is_free_order else ""),
            detected_terms=detected_terms,
            existing_terms_before=existing_terms_before,
            payment_terms_after=payment_terms_name(latest.get("paymentTerms")),
            freight_action=freight_action,
            freight_title=freight_title,
            freight_price=freight_price,
            order_id=order_id,
            order_name=order_name,
        )
    else:
        log_draft_result(
            latest,
            action="completed",
            success=True,
            reason="Draft completed successfully" + (" (free order)" if is_free_order else ""),
            detected_terms=detected_terms,
            existing_terms_before=existing_terms_before,
            payment_terms_after=payment_terms_name(latest.get("paymentTerms")),
            freight_action=freight_action,
            freight_title=freight_title,
            freight_price=freight_price,
            order_id=order_id,
            order_name=order_name,
        )


def main() -> None:
    now_dt = datetime.now(timezone.utc)

    # Side-channel reporting only — never allowed to block or fail the actual
    # order-processing run below, regardless of what goes wrong here.
    try:
        publish_excluded_skus_snapshot()
    except Exception:
        logger.exception("Could not publish excluded SKU snapshot (non-fatal, continuing run)")

    drafts = fetch_candidate_drafts()
    inventory_pool: InventoryPool = {}

    for draft in drafts:
        try:
            process_draft(draft, now_dt, inventory_pool)
        except Exception as exc:
            logger.exception("Failed processing %s: %s", draft.get("name"), exc)
            try:
                latest = recheck_draft(draft["id"])
                mark_needs_review(latest, str(exc))
                latest = recheck_draft(draft["id"])

                log_draft_result(
                    latest,
                    action="needs-review",
                    success=False,
                    reason=str(exc),
                    existing_terms_before=payment_terms_name(draft.get("paymentTerms")),
                    payment_terms_after=payment_terms_name(latest.get("paymentTerms")),
                    order_id=(latest.get("order") or {}).get("id", ""),
                    order_name=(latest.get("order") or {}).get("name", ""),
                )
            except Exception:
                logger.exception("Could not apply needs-review tag to %s", draft.get("name"))


if __name__ == "__main__":
    main()
