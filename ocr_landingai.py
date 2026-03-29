import json
import os
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv
from landingai_ade import LandingAIADE
from landingai_ade.lib import pydantic_to_json_schema

from invoice_schema import InvoiceExtract

load_dotenv()


def _build_output_path(path: Path, suffix: str, output_label: Optional[str]) -> Path:
    stem = f"{path.stem}.{output_label}" if output_label else path.stem
    return path.with_name(f"{stem}.{suffix}")


def _schema_with_hints(hint_fields: Optional[List[str]]) -> str:
    base_schema = json.loads(pydantic_to_json_schema(InvoiceExtract))
    normalized_hints = [str(field_name).strip() for field_name in (hint_fields or []) if str(field_name).strip()]
    if not normalized_hints:
        return json.dumps(base_schema)

    hint_text = (
        "Prioritize extracting these fields accurately from the document if present: "
        + ", ".join(normalized_hints)
        + "."
    )
    base_schema["description"] = (
        (base_schema.get("description") or "").strip() + " " + hint_text
    ).strip()
    properties = base_schema.get("properties") or {}
    for field_name in normalized_hints:
        field_schema = properties.get(field_name)
        if not isinstance(field_schema, dict):
            continue
        description = (field_schema.get("description") or "").strip()
        field_schema["description"] = (
            description + " Extraction hint: return the clearest available value for this field."
        ).strip()
    return json.dumps(base_schema)


def _markdown_with_hints(markdown: str, hint_fields: Optional[List[str]]) -> str:
    normalized_hints = [str(field_name).strip() for field_name in (hint_fields or []) if str(field_name).strip()]
    if not normalized_hints:
        return markdown
    hint_block = (
        "Extraction hint: pay special attention to these invoice fields and return the best value you can find for them: "
        + ", ".join(normalized_hints)
        + "."
    )
    return f"{hint_block}\n\n{markdown}"


def ocr_invoice_to_json(
    invoice_path: str,
    *,
    hint_fields: Optional[List[str]] = None,
    output_label: Optional[str] = None,
) -> str | None:
    api_key = os.getenv("VISION_AGENT_API_KEY")
    if not api_key:
        return None
    path = Path(invoice_path)
    if not path.exists():
        return None

    model_name = os.getenv("ADE_MODEL", "dpt-2-latest")
    environment = os.getenv("ADE_ENVIRONMENT", "production")
    if environment.lower() == "eu":
        client = LandingAIADE()
    else:
        client = LandingAIADE()

    parse_response = client.parse(document=path, model=model_name)
    parse_json_path = _build_output_path(path, "parse.json", output_label)
    with open(parse_json_path, "w", encoding="utf-8") as handle:
        json.dump(parse_response.to_dict(), handle, ensure_ascii=False, indent=2)

    schema = _schema_with_hints(hint_fields)
    extract_response = client.extract(
        schema=schema,
        markdown=_markdown_with_hints(parse_response.markdown, hint_fields),
    )
    extract_data = extract_response.extraction
    extract_json_path = _build_output_path(path, "fields.json", output_label)
    with open(extract_json_path, "w", encoding="utf-8") as handle:
        json.dump(extract_data, handle, ensure_ascii=False, indent=2)

    extract_meta_path = _build_output_path(path, "fields.meta.json", output_label)
    with open(extract_meta_path, "w", encoding="utf-8") as handle:
        json.dump(
            {
                "hint_fields": hint_fields or [],
                "extraction_metadata": extract_response.extraction_metadata,
                "metadata": extract_response.metadata.to_dict() if hasattr(extract_response.metadata, "to_dict") else {},
            },
            handle,
            ensure_ascii=False,
            indent=2,
        )
    return str(extract_json_path)
