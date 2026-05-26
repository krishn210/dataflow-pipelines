# =============================================================================
# PHI Agentic Data Platform — Generated Beam Pipeline
# =============================================================================
# Source ID:      src_customers_001
# Source Name:    Customer Master Data
# Source Type:    CSV
# Domain:         customer
# Mode:           BOOTSTRAP
# Generated at:   2026-05-26T07:42:30.981112Z
# Schema version: 1
# Target table:   anz-cloud-migration:phi_bronze_customer.src_customers_001_raw
# Dataplex asset: to-be-registered
#
# PII transforms (6 columns):   NationalIdentificationNumber (PII_NAME), PhoneNumber (PII_MOBILE), NationalIdentificationNumber_1 (PII_NAME), DateOfBirth (PII_DOB), PhoneNumber_1 (PII_MOBILE), DateOfBirth_1 (PII_DOB)
#      
#
# DO NOT EDIT MANUALLY.
# Regenerate by triggering the PHI Code Generation Agent with
# source_id=src_customers_001 after any schema change.
# =============================================================================


import argparse
import hashlib
import json
import logging
import os
import re
import sys
from datetime import datetime, date
from typing import Any, Dict, Iterator, Optional, Tuple

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToBigQuery
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.options.pipeline_options import (
    PipelineOptions, StandardOptions, GoogleCloudOptions,
    SetupOptions, WorkerOptions,
)
from apache_beam.metrics import Metrics
from apache_beam.pvalue import TaggedOutput

from google.cloud import dataplex_v1
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.data_catalog_v1 import PolicyTagManagerClient

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


# =============================================================================
# Constants — parameterised by Dataplex metadata at code-generation time
# =============================================================================

SOURCE_ID         = "src_customers_001"
SOURCE_NAME       = "Customer Master Data"
DOMAIN            = "customer"
SCHEMA_VERSION    = "1"
DATAPLEX_ASSET_ID = ""
BQ_PROJECT        = "anz-cloud-migration"
BQ_DATASET        = "phi_bronze_customer"
BQ_TABLE          = "src_customers_001_raw"
BQ_TABLE_FQN      = "anz-cloud-migration:phi_bronze_customer.src_customers_001_raw"

# Column definitions — schema as of version 1
COLUMN_ORDER = ['NationalIdentificationNumber', 'PersonName', 'PhoneNumber', 'AddressCity', 'ElectronicAddress', 'NationalIdentificationNumber_1', 'DateOfBirth', 'CustomerSegment', 'CustomerSegment_1', 'CustomerSegment_2', 'PartyRoleStatus', 'PersonName_1', 'PhoneNumber_1', 'CustomerIdentifier', 'ElectronicAddress_1', 'AddressCity_1', 'DateOfBirth_1']

# BigQuery schema with policy tag resource names embedded.
# BQ enforces column-level access based on these tags at query time.
BQ_SCHEMA = {"fields": [{'name': 'NationalIdentificationNumber', 'type': 'STRING', 'mode': 'NULLABLE', 'policyTags': {'names': ['projects/anz-cloud-migration/locations/us-central1/taxonomies/4502003670685444846/policyTags/3366224449682084667']}}, {'name': 'PersonName', 'type': 'STRING', 'mode': 'NULLABLE'}, {'name': 'PhoneNumber', 'type': 'STRING', 'mode': 'NULLABLE', 'policyTags': {'names': ['projects/anz-cloud-migration/locations/us-central1/taxonomies/4502003670685444846/policyTags/5296288691026736836']}}, {'name': 'AddressCity', 'type': 'STRING', 'mode': 'NULLABLE'}, {'name': 'ElectronicAddress', 'type': 'STRING', 'mode': 'NULLABLE'}, {'name': 'NationalIdentificationNumber_1', 'type': 'STRING', 'mode': 'NULLABLE', 'policyTags': {'names': ['projects/anz-cloud-migration/locations/us-central1/taxonomies/4502003670685444846/policyTags/3366224449682084667']}}, {'name': 'DateOfBirth', 'type': 'DATE', 'mode': 'NULLABLE', 'policyTags': {'names': ['projects/anz-cloud-migration/locations/us-central1/taxonomies/4502003670685444846/policyTags/4405612200200574405']}}, {'name': 'CustomerSegment', 'type': 'DATE', 'mode': 'NULLABLE'}, {'name': 'CustomerSegment_1', 'type': 'STRING', 'mode': 'NULLABLE'}, {'name': 'CustomerSegment_2', 'type': 'STRING', 'mode': 'NULLABLE'}, {'name': 'PartyRoleStatus', 'type': 'STRING', 'mode': 'NULLABLE'}, {'name': 'PersonName_1', 'type': 'STRING', 'mode': 'NULLABLE'}, {'name': 'PhoneNumber_1', 'type': 'STRING', 'mode': 'NULLABLE', 'policyTags': {'names': ['projects/anz-cloud-migration/locations/us-central1/taxonomies/4502003670685444846/policyTags/5296288691026736836']}}, {'name': 'CustomerIdentifier', 'type': 'STRING', 'mode': 'NULLABLE'}, {'name': 'ElectronicAddress_1', 'type': 'STRING', 'mode': 'NULLABLE'}, {'name': 'AddressCity_1', 'type': 'STRING', 'mode': 'NULLABLE'}, {'name': 'DateOfBirth_1', 'type': 'DATE', 'mode': 'NULLABLE', 'policyTags': {'names': ['projects/anz-cloud-migration/locations/us-central1/taxonomies/4502003670685444846/policyTags/4405612200200574405']}}]}

# PII masking rules — applied BEFORE any data reaches BigQuery
MASKING_RULES: Dict[str, str] = {'NationalIdentificationNumber': 'TOKENIZE', 'PhoneNumber': 'PARTIAL_MASK', 'NationalIdentificationNumber_1': 'TOKENIZE', 'DateOfBirth': 'YEAR_ONLY', 'PhoneNumber_1': 'PARTIAL_MASK', 'DateOfBirth_1': 'YEAR_ONLY'}

# Parsing hints from Schema Discovery Agent
PARSING_HINTS: Dict[str, Any] = {}

# Dataflow operational parameters
EXPECTED_DAILY_ROWS = 100000
VOLUME_CLASS        = "MEDIUM"
SLA_MINUTES         = 60

# Dead-letter tag name
DLQ_TAG = "dead_letter"


# =============================================================================
# PII Masking Functions
# =============================================================================

def mask_partial(value: Any, visible_tail: int = 4) -> Optional[str]:
    """Keep only the last N digits visible. Used for mobile numbers.
    Example: 9876543210 → ******3210"""
    if value is None:
        return None
    s = re.sub(r"[^\d]", "", str(value))
    if len(s) <= visible_tail:
        return "*" * len(s)
    return "*" * (len(s) - visible_tail) + s[-visible_tail:]

def mask_tokenize(value: Any) -> Optional[str]:
    """Replace name with a stable deterministic token.
    The token is consistent — same name always maps to same token,
    enabling analytics without exposing actual names."""
    if value is None or str(value).strip() == "":
        return None
    h = hashlib.sha256(str(value).strip().lower().encode("utf-8")).hexdigest()
    return "TOK_" + h[:16].upper()

def mask_year_only(value: Any) -> Optional[str]:
    """Retain only the year from a date of birth.
    Example: 1985-03-15 → 1985"""
    if value is None:
        return None
    s = str(value).strip()
    # Try common date patterns
    for pattern in [
        r"(\d{4})[\-/\.](\d{1,2})[\-/\.](\d{1,2})",  # 1985-03-15
        r"(\d{1,2})[\-/\.](\d{1,2})[\-/\.](\d{4})",  # 15-03-1985
        r"(\d{1,2})(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)(\d{4})",
        r"dob(\d{2})(\d{2})(\d{4})",  # dob15031985
        r"born (\d{4})",
    ]:
        m = re.search(pattern, s.lower())
        if m:
            # Find the 4-digit year group
            for g in m.groups():
                if g and len(g) == 4 and g.isdigit():
                    return g
    # Last resort: find any 4-digit year-like number
    m = re.search(r"\b(19\d{2}|20[01]\d)\b", s)
    return m.group(1) if m else None

def apply_masking(column_name: str, value: Any) -> Any:
    """
    Dispatch to the correct masking function based on MASKING_RULES.
    Called once per field per row during pipeline execution.
    """
    strategy = MASKING_RULES.get(column_name, "NONE")
    if strategy == "NONE":
        return value
    dispatch = {
        "PARTIAL_MASK": mask_partial,
        "TOKENIZE": mask_tokenize,
        "YEAR_ONLY": mask_year_only
    }
    fn = dispatch.get(strategy)
    return fn(value) if fn else value


# =============================================================================
# Source Connector — FILE (CSV)
# =============================================================================
# PHI source files use inconsistent delimiters within the SAME file:
#   pipe:       CLM001|POL001|hospitalization|75000|...
#   tilde ~~~:  agt010~~~kavita sharma~~~north~~~...
#   semicolon:  agt004;neha gupta;east;...
#   double ;;:  pol004;;cust004;;health;;...
#   asterisk:   AGT005***VIKRAM SHARMA***NORTH***...
#   key=value:  agent_code=AGT006 name=anita patel ...
#   slash:      AGT007/rohit verma/west/...
#   KEY:VAL,…:  NAME:NEHA GUPTA,PHONE:4321098765,...
#   space:      agt008 meera iyer south ...
#   AGENT#:     AGENT#AGT009#NAME#suresh kumar#...
# =============================================================================

POSITIONAL_COLUMNS = {k: v for k, v in enumerate(COLUMN_ORDER)}


class MultiFormatParser:
    """Detects the delimiter style of each row and returns
    a dict {column_name: raw_value} before any masking."""

    KV_EQUALS_RE = re.compile(
        r"(?:^|\s)(\w+)=((?:[^\s=]+(?:\s+[^\s=]+)*?)?)(?=\s+\w+=|$)"
    )
    KV_COLON_RE  = re.compile(r"([A-Z_]+):([^,]+)")
    KV_PREFIXED_RE = re.compile(
        r"(?:AGENT:|CLAIM:|POLICY:|CUSTOMER:)?(\w+)[:\s]+(.*?)(?=\s+\w+[:\s]|$)"
    )

    def parse(self, line: str) -> Optional[Dict[str, str]]:
        line = line.strip()
        if not line:
            return None
        row_dict = self._detect_and_parse(line)
        if row_dict is None:
            return None
        return {k.lower().strip(): str(v).strip() for k, v in row_dict.items()}

    def _detect_and_parse(self, line: str) -> Optional[Dict]:
        if "=" in line and not line.startswith("AGT") and not line.startswith("CLM"):
            result = self._parse_kv_equals(line)
            if result and len(result) >= 3:
                return result
        if ":" in line and "," in line and re.search(r"[A-Z_]{3,}:", line):
            result = self._parse_kv_colon_csv(line)
            if result and len(result) >= 3:
                return result
        if re.match(r"^(AGENT|CLAIM|POLICY|CUSTOMER):", line):
            result = self._parse_prefixed_kv(line)
            if result and len(result) >= 3:
                return result
        if "~~~" in line:
            return self._parse_positional(re.split(r"~~~", line))
        if ";;" in line:
            parts = re.split(r";;", line)
            return self._parse_positional([p for p in parts if p])
        if "***" in line:
            return self._parse_positional(re.split(r"\*{3}", line))
        if "|" in line and line.count("|") >= 2:
            return self._parse_positional(line.split("|"))
        if ";" in line and line.count(";") >= 2:
            return self._parse_positional(line.split(";"))
        if "~~" in line and "~~~" not in line:
            return self._parse_positional(re.split(r"~~", line))
        if "#" in line:
            result = self._parse_hash_alternating(line)
            if result:
                return result
        if "/" in line and line.count("/") >= 2:
            parts = [p for p in line.split("/") if p.strip()]
            if len(parts) >= 3:
                return self._parse_positional(parts)
        parts = line.split()
        if len(parts) >= len(COLUMN_ORDER):
            return self._parse_positional(parts)
        log.warning(f"Could not parse line: {line[:80]}")
        return None

    def _parse_positional(self, parts: list) -> Dict[str, str]:
        return {COLUMN_ORDER[i]: v.strip()
                 for i, v in enumerate(parts) if i < len(COLUMN_ORDER)}

    def _parse_kv_equals(self, line: str) -> Dict[str, str]:
        result = {}
        for token in re.split(r"\s+(?=\w+=)", line.strip()):
            if "=" in token:
                key, _, val = token.partition("=")
                result[key.strip()] = val.strip()
        return result

    def _parse_kv_colon_csv(self, line: str) -> Dict[str, str]:
        return {m.group(1).strip(): m.group(2).strip()
                 for m in self.KV_COLON_RE.finditer(line)}

    def _parse_prefixed_kv(self, line: str) -> Dict[str, str]:
        line = re.sub(r"^(AGENT|CLAIM|POLICY|CUSTOMER):", "", line).strip()
        result = {}
        for token in re.split(r"\s+(?=[A-Z_]{2,}:)", line):
            if ":" in token:
                key, _, val = token.partition(":")
                result[key.strip()] = val.strip()
        return result

    def _parse_hash_alternating(self, line: str) -> Optional[Dict[str, str]]:
        parts = line.split("#")
        if len(parts) < 4:
            return None
        result = {}
        col_map = {
            "AGENT": COLUMN_ORDER[0] if COLUMN_ORDER else "id",
            "NAME": "name", "REGION": "region",
            "JOIN": "join_date", "COMM": "commission_rate",
            "MGR": "manager_id", "TARGET": "target_amount",
            "ACHIEVED": "achieved_amount", "STATUS": "status",
            "CLAIM": "claim_id", "POLICY": "policy_id",
            "TYPE": "claim_type", "AMOUNT": "amount",
            "HOSPITAL": "hospital", "DATE": "claim_date",
            "PAID": "paid_amount", "CUSTOMER": "customer_id",
            "PREMIUM": "premium", "START": "start_date", "END": "end_date",
        }
        i = 0
        while i + 1 < len(parts):
            key = parts[i].strip()
            val = parts[i + 1].strip()
            if key and not key.isupper():
                break
            result[col_map.get(key, key.lower())] = val
            i += 2
        return result if len(result) >= 3 else None


PARSER = MultiFormatParser()


class SourceReader:
    """Uniform interface used by ParseAndNormaliseFn regardless of source type."""
    def parse(self, line: str) -> Optional[Dict[str, str]]:
        return PARSER.parse(line)


# =============================================================================
# Schema Validation and Type Coercion
# =============================================================================

MONTH_MAP = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12",
}


def coerce_date(value: Any) -> Optional[str]:
    """Normalise many date formats to YYYY-MM-DD."""
    if not value:
        return None
    s = str(value).strip().lower()

    # Already ISO: 2024-01-15 or 2024/01/15
    m = re.match(r"(\d{4})[\-/](\d{1,2})[\-/](\d{1,2})", s)
    if m:
        return f"{m.group(1)}-{m.group(2).zfill(2)}-{m.group(3).zfill(2)}"

    # DD/MM/YYYY or DD-MM-YYYY
    m = re.match(r"(\d{1,2})[\-/](\d{1,2})[\-/](\d{4})", s)
    if m:
        return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"

    # 15mar2020 or 01jan2024
    m = re.match(r"(\d{1,2})(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)(\d{4})", s)
    if m:
        return f"{m.group(3)}-{MONTH_MAP[m.group(2)]}-{m.group(1).zfill(2)}"

    # 15MAR2020
    m = re.match(r"(\d{1,2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d{4})", s.upper())
    if m:
        return f"{m.group(3)}-{MONTH_MAP[m.group(2).lower()]}-{m.group(1).zfill(2)}"

    # dob15031985
    m = re.match(r"dob(\d{2})(\d{2})(\d{4})", s)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"

    return None  # Unrecognised format


def clean_aadhaar(value: Any) -> Optional[str]:
    """Strip prefixes like 'aadhaar:', 'aadhar', 'AADHAR' and return 12-digit number."""
    if not value:
        return None
    s = re.sub(r"[aA][aA][dD][hH]?[aA][aA]?[rR]?[:=\s]*", "", str(value).strip())
    digits = re.sub(r"[^\d]", "", s)
    return digits if len(digits) == 12 else str(value)  # fallback to original


def clean_phone(value: Any) -> Optional[str]:
    """Strip prefixes like 'PH' and return digit string."""
    if not value:
        return None
    s = re.sub(r"^[Pp][Hh]", "", str(value).strip())
    return re.sub(r"[^\d+]", "", s) or None


def coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "").replace("%", "").strip())
    except (ValueError, TypeError):
        return None


def coerce_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(float(str(value).replace(",", "").strip()))
    except (ValueError, TypeError):
        return None


def clean_commission_rate(value: Any) -> Optional[float]:
    """Convert '12%' or '0.12' to 12.0 (as a percentage)."""
    if not value:
        return None
    s = str(value).strip().replace("%", "")
    try:
        f = float(s)
        return f if f > 1 else f * 100
    except (ValueError, TypeError):
        return None


def clean_status(value: Any) -> Optional[str]:
    return str(value).strip().lower() if value else None


# =============================================================================
# Pipeline Options
# =============================================================================

class PhiPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--source_path",   required=True,
                            help="GCS path or connection string for source data")
        parser.add_argument("--bq_project",    default=BQ_PROJECT)
        parser.add_argument("--bq_dataset",    default=BQ_DATASET)
        parser.add_argument("--bq_table",      default=BQ_TABLE)
        parser.add_argument("--run_id",        default="",
                            help="Unique run ID for lineage tracking")
        parser.add_argument("--dry_run",       default="false",
                            help="Parse but do not write to BQ")


# Dataflow worker configuration (generated from volume class = MEDIUM)
DATAFLOW_OPTS = {
    "num_workers":        4,
    "max_num_workers":    16,
    "machine_type":       "n1-standard-4",
    "disk_size_gb":       50,
    "autoscaling_algorithm": "THROUGHPUT_BASED",
}


            # =============================================================================
            # DoFns — individual Beam transforms
            # =============================================================================

            class ParseAndNormaliseFn(beam.DoFn):
                """
                Parses one raw record using SourceReader, applies type coercion
                and masking, then routes valid rows to main and failures to DLQ.
                SourceReader is generated per source_type — this DoFn is identical
                for all source types.
                """
                def __init__(self):
                    self.parse_errors   = Metrics.counter(self.__class__, "parse_errors")
                    self.rows_processed = Metrics.counter(self.__class__, "rows_processed")
                    self._reader = SourceReader()

                def process(self, raw):
                    raw_repr = str(raw)[:200]   # Safe representation for DLQ logging
                    row = self._reader.parse(raw)
                    if row is None:
                        self.parse_errors.inc()
                        yield TaggedOutput(DLQ_TAG, {
                            "raw_line":  raw_repr,
                            "source_id": SOURCE_ID,
                            "error":     "parse_failed",
                            "timestamp": datetime.utcnow().isoformat(),
                        })
                        return

                    try:
                        row = {
        "NationalIdentificationNumber": apply_masking("NationalIdentificationNumber", str(row.get("NationalIdentificationNumber", "") or "").strip() or None),
        "PersonName": str(row.get("PersonName", "") or "").strip() or None,
        "PhoneNumber": apply_masking("PhoneNumber", clean_phone(row.get("PhoneNumber", ""))),
        "AddressCity": str(row.get("AddressCity", "") or "").strip() or None,
        "ElectronicAddress": str(row.get("ElectronicAddress", "") or "").strip() or None,
        "NationalIdentificationNumber_1": apply_masking("NationalIdentificationNumber_1", str(row.get("NationalIdentificationNumber_1", "") or "").strip() or None),
        "DateOfBirth": apply_masking("DateOfBirth", coerce_date(row.get("DateOfBirth", ""))),
        "CustomerSegment": str(row.get("CustomerSegment", "") or "").strip() or None,
        "CustomerSegment_1": str(row.get("CustomerSegment_1", "") or "").strip() or None,
        "CustomerSegment_2": str(row.get("CustomerSegment_2", "") or "").strip() or None,
        "PartyRoleStatus": clean_status(row.get("PartyRoleStatus", "")),
        "PersonName_1": str(row.get("PersonName_1", "") or "").strip() or None,
        "PhoneNumber_1": apply_masking("PhoneNumber_1", clean_phone(row.get("PhoneNumber_1", ""))),
        "CustomerIdentifier": str(row.get("CustomerIdentifier", "") or "").strip() or None,
        "ElectronicAddress_1": str(row.get("ElectronicAddress_1", "") or "").strip() or None,
        "AddressCity_1": str(row.get("AddressCity_1", "") or "").strip() or None,
        "DateOfBirth_1": apply_masking("DateOfBirth_1", coerce_date(row.get("DateOfBirth_1", ""))),
                        }
                        self.rows_processed.inc()
                        yield row

                    except Exception as e:
                        self.parse_errors.inc()
                        yield TaggedOutput(DLQ_TAG, {
                            "raw_line":  raw_repr,
                            "source_id": SOURCE_ID,
                            "error":     f"transform_failed: {e}",
                            "timestamp": datetime.utcnow().isoformat(),
                        })


            class ValidateRowFn(beam.DoFn):
                """
                Basic row-level validation after parsing.
                Routes invalid rows to DLQ with reason.
                """
                # Which columns cannot be null — driven by schema confidence
                REQUIRED_COLS = []

                def process(self, row: dict):
                    missing = [c for c in self.REQUIRED_COLS if not row.get(c)]
                    if missing:
                        yield TaggedOutput(DLQ_TAG, {
                            "row":       row,
                            "source_id": SOURCE_ID,
                            "error":     f"missing_required: {missing}",
                            "timestamp": datetime.utcnow().isoformat(),
                        })
                        return
                    yield row




            # =============================================================================
            # Main pipeline function
            # =============================================================================

            def run(argv=None):
                parser = argparse.ArgumentParser()
                known_args, pipeline_args = parser.parse_known_args(argv)

                options = PipelineOptions(pipeline_args)
                phi_opts = options.view_as(PhiPipelineOptions)

                # ── Dataflow runner options ────────────────────────────────────────────
                gcp_opts = options.view_as(GoogleCloudOptions)
                gcp_opts.project = phi_opts.bq_project
                gcp_opts.job_name = f"phi-{DOMAIN}-src_customers_001-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}"

                worker_opts = options.view_as(WorkerOptions)
                worker_opts.num_workers    = DATAFLOW_OPTS["num_workers"]
                worker_opts.max_num_workers = DATAFLOW_OPTS["max_num_workers"]
                worker_opts.machine_type   = DATAFLOW_OPTS["machine_type"]
                worker_opts.disk_size_gb   = DATAFLOW_OPTS["disk_size_gb"]
                worker_opts.autoscaling_algorithm = DATAFLOW_OPTS["autoscaling_algorithm"]

                options.view_as(SetupOptions).save_main_session = True

                table_ref = f"{phi_opts.bq_project}:{phi_opts.bq_dataset}.{phi_opts.bq_table}"
                run_id    = phi_opts.run_id or datetime.utcnow().strftime("%Y%m%d%H%M%S")

                log.info(f"Starting pipeline: {SOURCE_NAME} → {table_ref}")
                log.info(f"Run ID: {run_id}, Source: {phi_opts.source_path}")

                with beam.Pipeline(options=options) as p:
                    # ── Read — source-type aware ──────────────────────────────────────
                    # source_type=CSV (category=file)
                    raw_records = (
    p | "ReadSource" >> ReadFromText(
        phi_opts.source_path,
        skip_header_lines=0,
    )
)

                    # ── Parse + Mask ──────────────────────────────────────────────────
                    parsed = (
                        raw_records
                        | "ParseAndNormalise" >> beam.ParDo(
                            ParseAndNormaliseFn()
                        ).with_outputs(DLQ_TAG, main="valid")
                    )

                    valid_rows = parsed.valid

                    # ── Validate ──────────────────────────────────────────────────────
                    validated = (
                        valid_rows
                        | "ValidateRows" >> beam.ParDo(
                            ValidateRowFn()
                        ).with_outputs(DLQ_TAG, main="good")
                    )

                    good_rows = validated.good

                    # ── Write to BigQuery (Bronze layer) ──────────────────────────────
                    if phi_opts.dry_run.lower() != "true":
                        good_rows | "WriteToBronze" >> WriteToBigQuery(
                            table=table_ref,
                            schema=BQ_SCHEMA,
                            write_disposition=BigQueryDisposition.WRITE_APPEND,
                            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                            # insert_retry_strategy ensures idempotency on redeployment
                            insert_retry_strategy=beam.io.gcp.bigquery_tools.RetryStrategy.RETRY_ON_TRANSIENT_ERROR,
                        )

                    # ── Dead-letter routing ───────────────────────────────────────────
                    dlq_table = f"{phi_opts.bq_project}:{phi_opts.bq_dataset}.{phi_opts.bq_table}_dlq"

                    # Merge DLQ outputs from both transforms
                    dlq_rows = (
                        (parsed[DLQ_TAG], validated[DLQ_TAG])
                        | "MergeDLQ" >> beam.Flatten()
                    )
                    dlq_rows | "WriteDLQ" >> WriteToBigQuery(
                        table=dlq_table,
                        schema={"fields": [
                            {"name": "raw_line",  "type": "STRING",    "mode": "NULLABLE"},
                            {"name": "row",       "type": "STRING",    "mode": "NULLABLE"},
                            {"name": "source_id", "type": "STRING",    "mode": "NULLABLE"},
                            {"name": "error",     "type": "STRING",    "mode": "NULLABLE"},
                            {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
                        ]},
                        write_disposition=BigQueryDisposition.WRITE_APPEND,
                        create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                    )

                log.info("Pipeline complete.")

                # Emit Dataplex lineage after successful run
                if run_id  and DATAPLEX_ASSET_ID:
                    emit_lineage(run_id, phi_opts.source_path, -1)


# =============================================================================
# Entrypoint
# =============================================================================

if __name__ == "__main__":
    import sys
    log.info(f"PHI Pipeline starting: {SOURCE_NAME} ({SOURCE_ID})")
    run(sys.argv[1:])
