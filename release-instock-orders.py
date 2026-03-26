import csv
import logging
import os
import re
import sys
from datetime import date, datetime, time, timedelta, timezone
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

READY_TAG = os.getenv("READY_TAG", "instock-ready").strip()
PROCESSING_TAG = os.getenv("PROCESSING_TAG", "order-push-processing").strip()
SUBMITTED_TAG = os.getenv("SUBMITTED_TAG", "order-submitted").strip()
NEEDS_REVIEW_TAG = os.getenv("NEEDS_REVIEW_TAG", "needs-review").strip()

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

EXCLUDED_CUSTOMERS = {
    c.strip().upper()
    for c in os.getenv(
        "EXCLUDED_CUSTOMERS",
        "Replacements Customer Care Customer Care",
    ).split(",")
    if c.strip()
}

DRY_RUN = os.getenv("DRY_RUN", "true").strip().lower() == "true"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper().strip()
DRAFTS_PAGE_SIZE = int(os.getenv("DRAFTS_PAGE_SIZE", "25").strip())

CSV_LOG_PATH = os.getenv("CSV_LOG_PATH", "release_instock_orders_log.csv").strip()

PAYMENT_TEMPLATE_MAP: Dict[int, str] = {
    30: os.getenv("PAYMENT_TERMS_TEMPLATE_ID_NET30", "").strip(),
    45: os.getenv("PAYMENT_TERMS_TEMPLATE_ID_NET45", "").strip(),
    60: os.getenv("PAYMENT_TERMS_TEMPLATE_ID_NET60", "").strip(),
    90: os.getenv("PAYMENT_TERMS_TEMPLATE_ID_NET90", "").strip(),
    120: os.getenv("PAYMENT_TERMS_TEMPLATE_ID_NET120", "").strip(),  # this should now be your Fixed template
}

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

if not SHOPIFY_SHOP or not SHOPIFY_TOKEN:
    logger.error("Missing required environment variables: SHOPIFY_SHOP and/or SHOPIFY_TOKEN")
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
      order {
        id
        name
      }
    }
    userErrors {
      field
      message
    }
  }
}
"""

DRAFT_COMPLETE_MUTATION = """
mutation CompleteDraftOrder($id: ID!) {
  draftOrderComplete(id: $id) {
    draftOrder {
      id
      name
      order {
        id
        name
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
    if not COMPLETE_DRAFT_NAMES:
        return True
    return normalize_draft_name(draft_name) in {n.upper() for n in COMPLETE_DRAFT_NAMES}


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
    return customer_name in EXCLUDED_CUSTOMERS


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


def log_draft_result(
    draft: dict,
    *,
    action: str,
    success: bool,
    reason: str = "",
    detected_terms: str = "",
    existing_terms_before: str = "",
    payment_terms_after: str = "",
    order_id: str = "",
    order_name: str = "",
) -> None:
    ship_date_value = ""
    if draft.get("metafield"):
        ship_date_value = draft["metafield"].get("value") or ""

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
        "final_tags": ",".join(normalize_tags(draft.get("tags", []))),
        "order_id": order_id,
        "order_name": order_name,
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
    if COMPLETE_DRAFT_NAMES:
        logger.info("COMPLETE_DRAFT_NAMES active: %s", sorted(COMPLETE_DRAFT_NAMES))
    return drafts


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


def complete_draft(draft_id: str) -> dict:
    if DRY_RUN:
        logger.info("DRY RUN | would complete draft %s", draft_id)
        return {}

    data = shopify_graphql(
        DRAFT_COMPLETE_MUTATION,
        {"id": draft_id},
    )
    user_errors = data["draftOrderComplete"].get("userErrors", [])
    if user_errors:
        raise RuntimeError(f"draftOrderComplete userErrors: {user_errors}")
    return data["draftOrderComplete"]["draftOrder"]


def claim_draft(draft: dict) -> None:
    current_tags = normalize_tags(draft.get("tags", []))
    claimed_tags = add_tags(current_tags, PROCESSING_TAG)
    update_draft(draft["id"], {"tags": claimed_tags})


def release_claim(draft: dict) -> None:
    current_tags = normalize_tags(draft.get("tags", []))
    released_tags = remove_tags(current_tags, PROCESSING_TAG)
    update_draft(draft["id"], {"tags": released_tags})


def mark_needs_review(draft: dict, reason: Optional[str] = None) -> None:
    current_tags = normalize_tags(draft.get("tags", []))
    final_tags = add_tags(current_tags, NEEDS_REVIEW_TAG)
    final_tags = remove_tags(final_tags, PROCESSING_TAG)
    update_draft(draft["id"], {"tags": final_tags})
    if reason:
        logger.warning("%s | marked %s | %s", draft.get("name"), NEEDS_REVIEW_TAG, reason)


def mark_submitted(draft: dict) -> None:
    current_tags = normalize_tags(draft.get("tags", []))
    final_tags = add_tags(current_tags, SUBMITTED_TAG)
    final_tags = remove_tags(
        final_tags,
        PROCESSING_TAG,
        NEEDS_REVIEW_TAG,
    )
    update_draft(draft["id"], {"tags": final_tags})


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
        # Net 120 is expected to use the Fixed template now.
        if "NET 30" in name or "NET30" in name:
            return False
        if "NET 45" in name or "NET45" in name:
            return False
        if "NET 60" in name or "NET60" in name:
            return False
        if "NET 90" in name or "NET90" in name:
            return False
        if "FIXED" in name:
            return True
        if due_in_days == 120:
            return True
        return False

    return False


def ensure_payment_terms(draft: dict, now_dt: datetime) -> Tuple[bool, str, str, Optional[int]]:
    existing = draft.get("paymentTerms")
    existing_name = payment_terms_name(existing)

    blob = build_note_blob(draft)
    detected_days = detect_net_terms_days(blob)
    detected_label = f"Net {detected_days}" if detected_days else ""

    if not detected_days:
        if existing:
            return True, f"No recognizable payment terms found in note/PO; keeping existing terms ({existing_name})", detected_label, detected_days
        return False, "No payment terms on draft and no recognizable net terms found in note/PO", detected_label, detected_days

    template_id = PAYMENT_TEMPLATE_MAP.get(detected_days, "").strip()
    if not template_id:
        return False, f"Detected Net {detected_days}, but no template ID is configured", detected_label, detected_days

    issued_at = build_issued_at(now_dt)

    if detected_days == 120:
        due_at = build_due_at(120, now_dt.date())
        payment_terms_payload = {
            "paymentTerms": {
                "paymentTermsTemplateId": template_id,
                "paymentSchedules": [
                    {
                        "issuedAt": issued_at,
                        "dueAt": due_at,
                    }
                ],
            }
        }
        logger.info(
            "Draft %s | overriding existing payment terms '%s' from note/PO to Net 120 using FIXED template %s with issuedAt %s and dueAt %s",
            draft["name"],
            existing_name or "NONE",
            template_id,
            issued_at,
            due_at,
        )
        update_draft(draft["id"], payment_terms_payload)
        return True, f"Overrode payment terms to Net 120 with issue date {issued_at} and due date {due_at}", detected_label, detected_days

    payment_terms_payload = {
        "paymentTerms": {
            "paymentTermsTemplateId": template_id,
            "paymentSchedules": [
                {
                    "issuedAt": issued_at,
                }
            ],
        }
    }

    logger.info(
        "Draft %s | overriding existing payment terms '%s' from note/PO to Net %s using template %s with issuedAt %s",
        draft["name"],
        existing_name or "NONE",
        detected_days,
        template_id,
        issued_at,
    )

    update_draft(draft["id"], payment_terms_payload)
    return True, f"Overrode payment terms to Net {detected_days} with issue date {issued_at}", detected_label, detected_days


def process_draft(draft: dict, now_dt: datetime) -> None:
    today = now_dt.date()
    name = draft["name"]
    draft_id = draft["id"]
    tags = normalize_tags(draft.get("tags", []))
    existing_terms_before = payment_terms_name(draft.get("paymentTerms"))

    if not should_process_draft(name):
        logger.info("Skipping %s because it is not in COMPLETE_DRAFT_NAMES", name)
        return

    if has_excluded_tag(tags):
        logger.info("Skipping %s because it has excluded tags", name)
        return

    if should_exclude_customer(draft):
        logger.info("Skipping %s because customer is excluded: %s", name, safe_company_name(draft))
        return

    logger.info("-----")
    logger.info("Evaluating %s", name)

    claim_draft(draft)

    latest = recheck_draft(draft_id)
    latest_tags = normalize_tags(latest.get("tags", []))

    if latest.get("order"):
        logger.info("%s already has an order; marking submitted", name)
        mark_submitted(latest)
        latest = recheck_draft(draft_id)
        log_draft_result(
            latest,
            action="already-submitted",
            success=True,
            reason="Draft already had an order",
            existing_terms_before=existing_terms_before,
            payment_terms_after=payment_terms_name(latest.get("paymentTerms")),
            order_id=(latest.get("order") or {}).get("id", ""),
            order_name=(latest.get("order") or {}).get("name", ""),
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
            terms_reason = f"Detected {detected_terms} in note/PO but payment terms still blank after update"
        elif not payment_terms_match_detected(latest.get("paymentTerms"), detected_days):
            terms_ok = False
            terms_reason = (
                f"Detected {detected_terms} in note/PO but Shopify returned "
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
        )
        return

    logger.info("%s | completing draft", name)
    completed = complete_draft(draft_id)

    order_id = ""
    order_name = ""
    if not DRY_RUN:
        order = completed.get("order") or {}
        order_id = order.get("id", "")
        order_name = order.get("name", "")
        logger.info(
            "%s | completed successfully -> order %s",
            name,
            order_name or order_id,
        )

    latest = recheck_draft(draft_id)
    mark_submitted(latest)
    latest = recheck_draft(draft_id)

    if DRY_RUN:
        log_draft_result(
            latest,
            action="dry-run-complete",
            success=True,
            reason="Draft would have been completed",
            detected_terms=detected_terms,
            existing_terms_before=existing_terms_before,
            payment_terms_after=payment_terms_name(latest.get("paymentTerms")),
            order_id=order_id,
            order_name=order_name,
        )
    else:
        log_draft_result(
            latest,
            action="completed",
            success=True,
            reason="Draft completed successfully",
            detected_terms=detected_terms,
            existing_terms_before=existing_terms_before,
            payment_terms_after=payment_terms_name(latest.get("paymentTerms")),
            order_id=order_id,
            order_name=order_name,
        )


def main() -> None:
    now_dt = datetime.now(timezone.utc)
    drafts = fetch_candidate_drafts()

    for draft in drafts:
        try:
            process_draft(draft, now_dt)
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
                )
            except Exception:
                logger.exception("Could not apply needs-review tag to %s", draft.get("name"))


if __name__ == "__main__":
    main()
