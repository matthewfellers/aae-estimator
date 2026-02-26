"""
AAE Scanner Pipeline v2.0 — Multi-Stage BOM Extraction
=======================================================
Replaces the old single-prompt scan_drawing() with a focused 4-stage pipeline:
  Stage 1: Detect BOM table structure & column headers
  Stage 2: Extract every BOM row (pure transcription)
  Stage 3: Derive estimator quantity buckets from BOM data
  Stage 4: Validate extraction (row count, column swap detection)

This eliminates hallucination by separating table-reading from interpretation.
"""

import json, re, time
from collections import Counter


def _call_claude(claude_client, pdf_b64, prompt, model="claude-sonnet-4-20250514",
                 thinking_budget=16000, max_tokens=16000):
    """Shared helper: call Claude with a PDF + text prompt, return parsed JSON.
    Handles extended thinking, JSON repair, markdown stripping."""

    api_kwargs = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": [{
            "role": "user",
            "content": [
                {
                    "type": "document",
                    "source": {
                        "type": "base64",
                        "media_type": "application/pdf",
                        "data": pdf_b64
                    }
                },
                {"type": "text", "text": prompt}
            ]
        }]
    }

    if "sonnet" in model and thinking_budget > 0:
        # API requires max_tokens > thinking.budget_tokens
        # Ensure max_tokens is always at least thinking_budget + output room
        if max_tokens <= thinking_budget:
            max_tokens = thinking_budget + max(8000, max_tokens)
            api_kwargs["max_tokens"] = max_tokens
            print(f"_call_claude: Bumped max_tokens to {max_tokens} "
                  f"(must exceed thinking_budget={thinking_budget})", flush=True)
        api_kwargs["thinking"] = {"type": "enabled", "budget_tokens": thinking_budget}
    elif "haiku" in model:
        api_kwargs["temperature"] = 0

    with claude_client.messages.stream(**api_kwargs) as stream:
        response = stream.get_final_message()

    stop_reason = response.stop_reason
    tokens_used = response.usage.output_tokens if response.usage else 0

    # Extract text block (skip thinking blocks)
    raw = ""
    for block in response.content:
        if block.type == "text":
            raw = block.text.strip()
            break
    if not raw:
        raw = response.content[0].text.strip()

    # Strip markdown fences
    raw = re.sub(r"^```json\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)

    # Parse JSON with repair fallback
    try:
        result = json.loads(raw)
    except json.JSONDecodeError:
        print(f"_call_claude: JSON truncated, attempting repair (len={len(raw)})", flush=True)
        last_brace = raw.rfind('}')
        if last_brace > 0:
            trimmed = raw[:last_brace+1]
            opens_b  = trimmed.count('{') - trimmed.count('}')
            opens_sq = trimmed.count('[') - trimmed.count(']')
            trimmed += ']' * opens_sq + '}' * opens_b
            try:
                result = json.loads(trimmed)
                result["_truncated"] = True
            except json.JSONDecodeError as e2:
                raise json.JSONDecodeError(
                    f"JSON parse failed after repair: {e2.msg} (len={len(raw)})",
                    e2.doc, e2.pos
                )
        else:
            raise

    result["_stop_reason"] = stop_reason
    result["_output_tokens"] = tokens_used
    if stop_reason == "max_tokens":
        result["_truncated"] = True

    return result


# ---------------------------------------------------------------------------
# Stage 1: Detect BOM table structure
# ---------------------------------------------------------------------------
def _stage1_detect_structure(claude_client, pdf_b64):
    """Find BOM tables, read column headers exactly, count rows.
    This is a quick, focused call -- no data extraction yet."""

    prompt = (
        "You are reading an industrial electrical panel drawing PDF.\n"
        "Your ONLY job is to find the BOM (Bill of Materials) table and report its structure.\n\n"
        "DO NOT extract any row data yet. Just report the table structure.\n\n"
        "Look through every page. Find any table that lists parts/components with quantities.\n\n"
        "Return ONLY this JSON -- no markdown, no explanation:\n"
        "{\n"
        '  "bom_tables_found": 1,\n'
        '  "drawing_types_found": ["BOM"],\n'
        '  "pages_with_bom": [3],\n'
        '  "column_headers_left_to_right": ["ITEM", "QTY", "CATALOG NO.", "MFG", "DESCRIPTION"],\n'
        '  "column_mapping": {\n'
        '    "item_num": "ITEM",\n'
        '    "qty": "QTY",\n'
        '    "part_number": "CATALOG NO.",\n'
        '    "manufacturer": "MFG",\n'
        '    "description": "DESCRIPTION"\n'
        "  },\n"
        '  "total_bom_rows": 0,\n'
        '  "has_manufacturer_column": true,\n'
        '  "has_description_column": true,\n'
        '  "notes": ""\n'
        "}\n\n"
        "Rules for column identification:\n"
        '- PART NUMBER column: contains codes like "1769-L33ER", "22B-D010N104" (alphanumeric with dashes/slashes)\n'
        '- MANUFACTURER column: short company names like "ALLEN BRADLEY", "PHOENIX CONTACT" (1-3 words)\n'
        '- DESCRIPTION column: LONGEST text -- full specs like "COMPACTLOGIX, 750KB, 16DI/16DO, 24VDC"\n'
        "- QTY column: small integers (1, 2, 5, 32)\n"
        "- ITEM column: sequential row numbers (1, 2, 3...)\n"
        "- If a column header is ambiguous, read 2-3 cells below it to determine what data type it holds\n\n"
        "Count total_bom_rows by counting every data row in the table (not headers, not blank rows).\n"
        "Count CAREFULLY -- go row by row and count each one. This number is critical for verification."
    )

    result = _call_claude(claude_client, pdf_b64, prompt, thinking_budget=10000, max_tokens=4000)
    print(f"SCAN Stage 1: Found {result.get('bom_tables_found', 0)} BOM table(s), "
          f"{result.get('total_bom_rows', '?')} rows, "
          f"headers={result.get('column_headers_left_to_right', [])}", flush=True)
    return result


# ---------------------------------------------------------------------------
# Stage 2: Extract every BOM row
# ---------------------------------------------------------------------------
def _stage2_extract_bom(claude_client, pdf_b64, structure):
    """Given the table structure from Stage 1, extract every row.
    This is pure transcription -- no interpretation, no classification."""

    col_map = structure.get("column_mapping", {})
    headers = structure.get("column_headers_left_to_right", [])
    total_rows = structure.get("total_bom_rows", 0)
    has_mfg = structure.get("has_manufacturer_column", True)
    has_desc = structure.get("has_description_column", True)

    # Build a dynamic prompt using the actual column mapping from Stage 1
    col_info = f"The column headers from left to right are: {headers}"
    mapping_lines = []
    for k, v in col_map.items():
        if v:
            mapping_lines.append(f'  - Column "{v}" -> field "{k}"')
    mapping_info = "\n".join(mapping_lines)

    prompt = (
        "You are transcribing the BOM (Bill of Materials) table from an electrical panel drawing.\n\n"
        f"COLUMN STRUCTURE (already identified):\n{col_info}\n\n"
        f"FIELD MAPPING:\n{mapping_info}\n\n"
        f"Has manufacturer column: {has_mfg}\n"
        f"Has description column: {has_desc}\n"
        f"Expected row count: {total_rows}\n\n"
        "YOUR TASK: Read EVERY row of the BOM table, going LEFT TO RIGHT across each row.\n"
        "Copy each cell value EXACTLY as printed -- character for character.\n\n"
        "CRITICAL RULES:\n"
        "1. ONE ROW = ONE ITEM. If the QTY column says 32, output qty:32 -- do NOT create 32 separate items.\n"
        "2. PART NUMBERS ARE THE MOST IMPORTANT FIELD. We will ORDER parts using these numbers.\n"
        '   Copy EVERY character EXACTLY: dashes, slashes, letters, numbers, spaces.\n'
        '   Examples of what matters: "5069-L306ERS2" is NOT "5069-L306ERS" (missing 2).\n'
        '   "2090-CSBM1DG-14LN03" is NOT "2090-CSBM1DG-14LN3" (missing 0).\n'
        '   "9861/15-04/12" has slashes and dashes -- copy them EXACTLY.\n'
        '   If you cannot read even ONE character clearly, write "[UNREADABLE]" for the whole part number.\n'
        "   NEVER reconstruct a part number from your training data. That is hallucination.\n"
        '3. If there is no manufacturer column, leave manufacturer as empty string "".\n'
        '4. If there is no description column, leave description as empty string "".\n'
        "5. Do NOT skip ANY rows. Every physical row in the table = one item in your output.\n"
        '6. For qty values like "A/R", "AR", "REF", use qty:1 and put the original text in notes.\n'
        "7. Read the ACTUAL cells -- do not use your knowledge of electrical parts to correct anything.\n"
        "   Even if you KNOW a part number looks wrong, copy what the drawing says. The drawing is the source of truth.\n\n"
        "SELF-CHECK after each row:\n"
        "  - Is description a long spec string? (Good) Or just 1-2 words like a company name? (Wrong -- you swapped columns)\n"
        "  - Is manufacturer a short company name? (Good) Or a long spec string? (Wrong -- you swapped columns)\n"
        "  - Does the part number appear somewhere in the PDF? (Good) Or did you generate it from memory? (Wrong)\n\n"
        "Return ONLY this JSON -- no markdown, no explanation:\n"
        "{\n"
        '  "bom_line_items": [\n'
        "    {\n"
        '      "item_num": 1,\n'
        '      "qty": 1,\n'
        '      "part_number": "",\n'
        '      "manufacturer": "",\n'
        '      "description": "",\n'
        '      "unit": "ea",\n'
        '      "notes": ""\n'
        "    }\n"
        "  ],\n"
        f'  "rows_extracted": 0\n'
        "}\n\n"
        f"Set rows_extracted to the actual count of items you return. It MUST equal {total_rows}.\n"
        "If it doesn't match, you missed rows -- go back and find them."
    )

    # Use generous thinking and output budgets for large tables
    # More thinking = more careful character-by-character reading
    think_budget = max(32000, total_rows * 600)  # more rows = more thinking needed
    think_budget = min(think_budget, 100000)  # cap at 100K
    max_out = max(20000, total_rows * 350)  # ~350 tokens per row for safety
    max_out = min(max_out, 128000)  # cap at 128K

    print(f"SCAN Stage 2: Extracting {total_rows} rows "
          f"(thinking={think_budget}, max_tokens={max_out})", flush=True)
    result = _call_claude(claude_client, pdf_b64, prompt,
                          thinking_budget=think_budget, max_tokens=max_out)

    items = result.get("bom_line_items", [])
    rows_reported = result.get("rows_extracted", len(items))
    print(f"SCAN Stage 2: Got {len(items)} items (model reported {rows_reported})", flush=True)

    return result


# ---------------------------------------------------------------------------
# Stage 3: Derive estimator quantities from BOM
# ---------------------------------------------------------------------------
def _stage3_derive_quantities(claude_client, pdf_b64, bom_items):
    """Classify BOM items into estimator quantity buckets.
    Uses a quick AI call to interpret component types from the BOM data."""

    # Build a compact text summary of the BOM for classification
    bom_summary_lines = []
    for item in bom_items:
        pn = item.get("part_number", "")
        mfr = item.get("manufacturer", "")
        desc = item.get("description", "")
        qty = item.get("qty", 1)
        bom_summary_lines.append(f"  qty={qty} | pn={pn} | mfg={mfr} | desc={desc}")
    bom_text = "\n".join(bom_summary_lines)

    prompt = (
        "You are an expert electrical estimator. Given this BOM extracted from a panel drawing,\n"
        "classify each component and fill out the estimator quantity fields.\n\n"
        f"BOM DATA:\n{bom_text}\n\n"
        "Also look at the full drawing PDF for additional information NOT in the BOM table:\n"
        "- Terminal schedules (for terminal block counts)\n"
        "- Wire schedules (for wire count)\n"
        "- Schematics (for relay/contactor counts not in BOM)\n"
        "- I/O lists (for PLC point counts)\n\n"
        "Return ONLY this JSON -- no markdown, no explanation:\n"
        "{\n"
        '  "quantities": {\n'
        '    "enc_qty": 0, "din_rail_runs": 0, "wire_duct_runs": 0, "enc_accessories": 0,\n'
        '    "main_amp": 0, "main_disconnect_type": "",\n'
        '    "branch_1p": 0, "branch_2p": 0, "branch_3p": 0,\n'
        '    "fused_disconnects": 0, "cpt_present": "N", "cpt_kva": 0, "pdb_qty": 0,\n'
        '    "relay_icecube": 0, "relay_din": 0,\n'
        '    "contactor_small": 0, "contactor_large": 0, "overload": 0,\n'
        '    "timers": 0, "ssrs": 0,\n'
        '    "pilot_lights": 0, "selectors": 0, "push_buttons": 0, "estops": 0,\n'
        '    "vfd_small": 0, "vfd_med": 0, "vfd_large": 0,\n'
        '    "soft_starter_small": 0, "soft_starter_large": 0,\n'
        '    "plc_present": "N", "plc_manufacturer": "", "plc_model": "",\n'
        '    "plc_di": 0, "plc_do": 0, "plc_ai": 0, "plc_ao": 0,\n'
        '    "hmi_present": "N", "hmi_size": 0,\n'
        '    "safety_relay": "N", "eth_switch": "N", "eth_cables": 0,\n'
        '    "tb_standard": 0, "tb_ground": 0, "tb_fused": 0, "tb_disconnect": 0,\n'
        '    "wire_count": 0, "wire_avg_len": 24\n'
        "  },\n"
        '  "category_assignments": [\n'
        '    {"item_num": 1, "category": "Enclosure"}\n'
        "  ]\n"
        "}\n\n"
        "Classification rules:\n"
        "- VFDs: <=5HP = vfd_small, 6-25HP = vfd_med, 26-100HP = vfd_large\n"
        "- Contactors: <=40A = contactor_small, >40A = contactor_large\n"
        "- Soft starters: <=50A = soft_starter_small, >50A = soft_starter_large\n"
        "- Count wire numbers if a wire schedule exists for wire_count\n"
        "- If no wire schedule: estimate wire_count as (DI+DO)*0.8 + (AI+AO)*1.0 + terminals*0.6\n"
        "- Categories: Enclosure, Power, Motor Ctrl, Control Devices, PLC/Network, Terminals, "
        "Relays, Wiring, HMI/Computer, Markers, Other\n"
        "- Use the BOM data above -- do NOT invent components that aren't listed\n"
        "- enc_qty: count enclosures in BOM (look for enclosure, cabinet, NEMA, SCE-)\n"
        "- din_rail_runs: if BOM lists DIN rail, use that qty; else estimate 1 per 12in of enclosure width\n"
        "- wire_duct_runs: if BOM lists wire duct/Panduit, use that qty; else estimate"
    )

    result = _call_claude(claude_client, pdf_b64, prompt,
                          thinking_budget=12000, max_tokens=8000)
    print(f"SCAN Stage 3: Quantities derived", flush=True)
    return result


# ---------------------------------------------------------------------------
# Stage 4: Validate extraction (deterministic, no AI)
# ---------------------------------------------------------------------------
def _stage4_validate(structure, bom_items):
    """Deterministic validation -- no AI call needed.
    Checks for common extraction errors and flags them."""

    flags = []
    expected_rows = structure.get("total_bom_rows", 0)
    actual_rows = len(bom_items)

    # Check 1: Row count mismatch
    if expected_rows > 0 and actual_rows != expected_rows:
        diff = expected_rows - actual_rows
        if diff > 0:
            flags.append(
                f"Row count mismatch: drawing has {expected_rows} rows "
                f"but only {actual_rows} extracted ({diff} missing)")
        else:
            flags.append(
                f"Row count mismatch: drawing has {expected_rows} rows "
                f"but {actual_rows} extracted ({-diff} extra)")

    # Check 2: Column swap detection
    known_mfrs = {
        "allen bradley", "allen-bradley", "rockwell", "siemens", "schneider",
        "phoenix contact", "saginaw", "abb", "eaton", "square d", "hoffman",
        "nvent", "panduit", "rittal", "hammond", "moxa", "turck", "red lion",
        "weidmuller", "mean well", "wago", "bussmann", "mersen",
        "idec", "omron", "automation direct", "banner", "pepperl", "pilz",
    }
    swap_count = 0
    for item in bom_items:
        desc = (item.get("description") or "").strip().lower()
        mfr = (item.get("manufacturer") or "").strip().lower()
        # Description is suspiciously short and looks like a company name
        if desc and len(desc.split()) <= 3 and any(m in desc for m in known_mfrs):
            swap_count += 1
        # Manufacturer is suspiciously long (>5 words)
        if mfr and len(mfr.split()) > 5:
            swap_count += 1

    if swap_count > 0:
        if swap_count > len(bom_items) * 0.3:  # >30% of rows affected
            flags.append(
                f"COLUMN SWAP DETECTED: {swap_count} items appear to have "
                "manufacturer/description swapped. Auto-correcting.")
            # Auto-correct
            for item in bom_items:
                desc = (item.get("description") or "").strip()
                mfr = (item.get("manufacturer") or "").strip()
                desc_looks_like_mfr = (
                    len(desc.split()) <= 3
                    and any(m in desc.lower() for m in known_mfrs)
                )
                mfr_looks_like_desc = len(mfr.split()) > 5
                if desc_looks_like_mfr or mfr_looks_like_desc:
                    item["description"], item["manufacturer"] = mfr, desc
                    item["notes"] = (
                        (item.get("notes", "") + " [auto-swapped mfr/desc]").strip()
                    )
        else:
            flags.append(
                f"Possible column swap on {swap_count} item(s) -- "
                "review manufacturer/description fields")

    # Check 3: Duplicate part numbers suggesting qty expansion error
    pn_counts = Counter(
        item.get("part_number", "")
        for item in bom_items
        if item.get("part_number")
    )
    for pn, count in pn_counts.most_common(5):
        if count >= 5 and pn != "[UNREADABLE]":
            flags.append(
                f"Part number '{pn}' appears {count} times -- verify these "
                f"aren't qty expansion errors (should be 1 row with qty={count}?)")

    # Check 4: Blank part numbers
    blank_pns = sum(
        1 for item in bom_items
        if not (item.get("part_number") or "").strip()
        or item.get("part_number") == "[UNREADABLE]"
    )
    if blank_pns > 0:
        flags.append(
            f"{blank_pns} item(s) have blank or unreadable part numbers -- "
            "verify against drawing")

    # Check 5: All qty=1 might indicate qty column wasn't read
    if len(bom_items) > 5:
        all_qty_1 = all(int(item.get("qty", 1)) == 1 for item in bom_items)
        if all_qty_1:
            flags.append(
                "All items have qty=1 -- verify the QTY column was read correctly")

    return flags


# ---------------------------------------------------------------------------
# Stage 5: AI-powered part number verification
# ---------------------------------------------------------------------------
def _stage5_verify_part_numbers(claude_client, pdf_b64, bom_items):
    """Send the extracted part numbers back to Claude and ask it to verify
    each one exists VERBATIM in the PDF. This catches hallucinated part numbers
    that look plausible but were generated from the model's training data
    rather than read from the actual drawing.

    Returns a list of corrections: [{item_num, original, corrected, note}]
    """

    # Build a compact list of part numbers to verify
    pn_lines = []
    for item in bom_items:
        pn = item.get("part_number", "")
        item_num = item.get("item_num", "?")
        if pn and pn != "[UNREADABLE]":
            pn_lines.append(f"  Item {item_num}: {pn}")
    pn_list = "\n".join(pn_lines)

    prompt = (
        "I extracted these part numbers from the BOM table in this drawing PDF.\n"
        "Your job: look at the ACTUAL PDF and verify each part number.\n\n"
        "EXTRACTED PART NUMBERS:\n"
        f"{pn_list}\n\n"
        "For EACH part number above:\n"
        "1. Find it in the actual BOM table in the PDF\n"
        "2. Compare character by character\n"
        "3. If it matches exactly, mark it verified\n"
        "4. If ANY character is wrong (even one digit or letter), provide the CORRECT value from the PDF\n"
        "5. If you cannot find it in the PDF at all, mark it as not_found\n\n"
        "Return ONLY this JSON -- no markdown, no explanation:\n"
        "{\n"
        '  "verifications": [\n'
        "    {\n"
        '      "item_num": 1,\n'
        '      "extracted": "5069-L306ERS2",\n'
        '      "status": "verified",\n'
        '      "corrected": "",\n'
        '      "note": ""\n'
        "    },\n"
        "    {\n"
        '      "item_num": 2,\n'
        '      "extracted": "5069-IB16X",\n'
        '      "status": "corrected",\n'
        '      "corrected": "5069-IB16",\n'
        '      "note": "Extra X at end was hallucinated"\n'
        "    }\n"
        "  ],\n"
        '  "total_verified": 0,\n'
        '  "total_corrected": 0,\n'
        '  "total_not_found": 0\n'
        "}\n\n"
        "Status must be one of: verified, corrected, not_found\n"
        "ONLY mark as verified if the part number is EXACTLY correct, character for character.\n"
        "This is critical -- wrong part numbers mean wrong parts get ordered."
    )

    result = _call_claude(
        claude_client, pdf_b64, prompt,
        thinking_budget=20000,
        max_tokens=max(8000, len(bom_items) * 150),
    )

    verifications = result.get("verifications", [])
    corrected_count = sum(1 for v in verifications if v.get("status") == "corrected")
    not_found_count = sum(1 for v in verifications if v.get("status") == "not_found")
    verified_count = sum(1 for v in verifications if v.get("status") == "verified")

    print(
        f"SCAN Stage 5: Verification complete -- "
        f"{verified_count} verified, {corrected_count} corrected, "
        f"{not_found_count} not found",
        flush=True,
    )

    return result


def _apply_corrections(bom_items, verification_result):
    """Apply part number corrections from Stage 5 back to the BOM items.
    Returns (corrected_items, correction_flags)."""

    flags = []
    verifications = verification_result.get("verifications", [])

    # Build lookup by item_num
    corrections = {}
    for v in verifications:
        item_num = v.get("item_num")
        if item_num is not None:
            corrections[item_num] = v

    corrected_count = 0
    not_found_count = 0

    for item in bom_items:
        item_num = item.get("item_num")
        if item_num in corrections:
            v = corrections[item_num]
            status = v.get("status", "")

            if status == "corrected" and v.get("corrected"):
                old_pn = item.get("part_number", "")
                new_pn = v["corrected"]
                item["part_number"] = new_pn
                note = v.get("note", "")
                item["notes"] = (
                    (item.get("notes", "") +
                     f" [PN corrected: {old_pn} -> {new_pn}]").strip()
                )
                corrected_count += 1

            elif status == "not_found":
                item["notes"] = (
                    (item.get("notes", "") +
                     " [WARNING: PN not verified in drawing]").strip()
                )
                not_found_count += 1

    if corrected_count > 0:
        flags.append(
            f"Part number verification: {corrected_count} part number(s) "
            "were corrected after cross-checking against the drawing"
        )
    if not_found_count > 0:
        flags.append(
            f"Part number verification: {not_found_count} part number(s) "
            "could not be verified -- review these manually"
        )

    return bom_items, flags


# ---------------------------------------------------------------------------
# Main entry point: scan_drawing()
# ---------------------------------------------------------------------------
def scan_drawing(claude_client, pdf_b64, filename="drawing.pdf"):
    """Multi-stage BOM extraction pipeline.
    Returns the same JSON contract as the old single-prompt version so the
    frontend and /api/scan endpoint don't need any changes.
    """

    model = "claude-sonnet-4-20250514"
    total_tokens = 0
    was_truncated = False
    all_flags = []

    try:
        # == STAGE 1: Detect table structure ================================
        print(f"SCAN [{filename}]: Starting Stage 1 -- structure detection", flush=True)
        try:
            structure = _stage1_detect_structure(claude_client, pdf_b64)
        except Exception as e1:
            err_str = str(e1)
            if "429" in err_str or "rate_limit" in err_str.lower() or "overloaded" in err_str.lower():
                if "input tokens per minute" in err_str.lower():
                    return {
                        "error": "token_rate_limit",
                        "error_message": (
                            "This PDF is too large for your current Anthropic API tier. "
                            "Your rate limit is 30,000 input tokens/minute but this document needs more. "
                            "To fix: go to console.anthropic.com -> Settings -> Billing -> load more credit "
                            "to increase your tier. Or try scanning a smaller PDF."
                        ),
                        "quantities": {}, "bom_line_items": [],
                        "extraction_summary": {
                            "confidence": 0,
                            "scope_gap_flags": ["token_rate_limit"],
                            "review_flags": ["PDF exceeds API tier token limit."],
                        },
                    }
                # Regular rate limit -- wait and retry once
                print(f"SCAN: Rate limit on Stage 1, waiting 30s...", flush=True)
                time.sleep(30)
                try:
                    structure = _stage1_detect_structure(claude_client, pdf_b64)
                except Exception:
                    raise e1
            else:
                raise

        total_tokens += structure.get("_output_tokens", 0)

        bom_count = structure.get("bom_tables_found", 0)
        row_count = structure.get("total_bom_rows", 0)
        drawing_types = structure.get("drawing_types_found", [])

        # No BOM table found -- still try to derive quantities from schematics
        if bom_count == 0 or row_count == 0:
            print(f"SCAN: No BOM table found -- skipping to Stage 3 for quantity-only scan", flush=True)
            all_flags.append("No BOM table found in drawing -- quantities estimated from schematics only")
            try:
                qty_result = _stage3_derive_quantities(claude_client, pdf_b64, [])
                total_tokens += qty_result.get("_output_tokens", 0)
                quantities = qty_result.get("quantities", {})
            except Exception:
                quantities = {}
            return {
                "column_mapping": {"detected_headers": [], "mapping": {}},
                "extraction_summary": {
                    "drawing_types_found": drawing_types,
                    "confidence": 0.4,
                    "scope_gap_flags": ["no_bom_table"],
                    "review_flags": all_flags,
                    "total_bom_rows_on_drawing": 0,
                },
                "quantities": quantities,
                "bom_line_items": [],
                "_model_used": model,
                "_stop_reason": "end_turn",
                "_output_tokens": total_tokens,
            }

        # == STAGE 2: Extract BOM rows ======================================
        print(f"SCAN [{filename}]: Starting Stage 2 -- extracting {row_count} rows", flush=True)
        try:
            extraction = _stage2_extract_bom(claude_client, pdf_b64, structure)
        except Exception as e2:
            err_str = str(e2)
            if "429" in err_str or "rate_limit" in err_str.lower() or "overloaded" in err_str.lower():
                print(f"SCAN: Rate limit on Stage 2, waiting 30s...", flush=True)
                time.sleep(30)
                try:
                    extraction = _stage2_extract_bom(claude_client, pdf_b64, structure)
                except Exception:
                    raise e2
            else:
                raise

        total_tokens += extraction.get("_output_tokens", 0)
        was_truncated = was_truncated or extraction.get("_truncated", False)
        bom_items = extraction.get("bom_line_items", [])

        # == STAGE 4: Validate (before Stage 3 so we fix issues first) ======
        print(f"SCAN [{filename}]: Starting Stage 4 -- validation", flush=True)
        validation_flags = _stage4_validate(structure, bom_items)
        all_flags.extend(validation_flags)

        # == STAGE 5: Verify part numbers against PDF =======================
        print(f"SCAN [{filename}]: Starting Stage 5 -- part number verification", flush=True)
        try:
            verify_result = _stage5_verify_part_numbers(
                claude_client, pdf_b64, bom_items
            )
            total_tokens += verify_result.get("_output_tokens", 0)
            bom_items, correction_flags = _apply_corrections(
                bom_items, verify_result
            )
            all_flags.extend(correction_flags)
        except Exception as e5:
            err_str = str(e5)
            if "429" in err_str or "rate_limit" in err_str.lower():
                print(f"SCAN: Rate limit on Stage 5, waiting 30s...", flush=True)
                time.sleep(30)
                try:
                    verify_result = _stage5_verify_part_numbers(
                        claude_client, pdf_b64, bom_items
                    )
                    total_tokens += verify_result.get("_output_tokens", 0)
                    bom_items, correction_flags = _apply_corrections(
                        bom_items, verify_result
                    )
                    all_flags.extend(correction_flags)
                except Exception:
                    # Non-fatal -- we still have the BOM, just unverified
                    print(f"SCAN: Stage 5 failed, proceeding with unverified PNs", flush=True)
                    all_flags.append(
                        "Part number verification skipped due to rate limit -- "
                        "review all part numbers manually"
                    )
            else:
                print(f"SCAN: Stage 5 error: {err_str}, continuing unverified", flush=True)
                all_flags.append(
                    "Part number verification failed -- review all part numbers manually"
                )

        # == STAGE 3: Derive quantities =====================================
        print(f"SCAN [{filename}]: Starting Stage 3 -- deriving quantities", flush=True)
        try:
            qty_result = _stage3_derive_quantities(claude_client, pdf_b64, bom_items)
        except Exception as e3:
            err_str = str(e3)
            if "429" in err_str or "rate_limit" in err_str.lower():
                print(f"SCAN: Rate limit on Stage 3, waiting 30s...", flush=True)
                time.sleep(30)
                try:
                    qty_result = _stage3_derive_quantities(claude_client, pdf_b64, bom_items)
                except Exception:
                    # Non-fatal -- we still have the BOM
                    print(f"SCAN: Stage 3 failed, returning BOM without quantities", flush=True)
                    qty_result = {"quantities": {}}
            else:
                # Non-rate-limit errors also non-fatal for Stage 3
                print(f"SCAN: Stage 3 error: {err_str}, continuing with BOM only", flush=True)
                qty_result = {"quantities": {}}

        total_tokens += qty_result.get("_output_tokens", 0)
        quantities = qty_result.get("quantities", {})

        # Apply category assignments from Stage 3 to BOM items
        cat_map = {}
        for ca in qty_result.get("category_assignments", []):
            cat_map[ca.get("item_num")] = ca.get("category", "Other")
        for item in bom_items:
            if item.get("item_num") in cat_map:
                item["category"] = cat_map[item["item_num"]]
            elif "category" not in item or not item["category"]:
                item["category"] = "Other"

        # == Assemble final result ==========================================
        col_map_raw = structure.get("column_mapping", {})
        detected_headers = structure.get("column_headers_left_to_right", [])
        column_mapping = {
            "detected_headers": detected_headers,
            "mapping": {v: k for k, v in col_map_raw.items() if v},
        }

        # Compute confidence based on extraction quality
        confidence = 0.9  # start high
        if was_truncated:
            confidence -= 0.2
            all_flags.append("Response was truncated -- some BOM items may be missing.")
        if len(bom_items) < row_count and row_count > 0:
            miss_pct = (row_count - len(bom_items)) / row_count
            confidence -= min(0.3, miss_pct)
        if any("COLUMN SWAP" in f for f in all_flags):
            confidence -= 0.1
        blank_pns = sum(
            1 for i in bom_items
            if not (i.get("part_number") or "").strip()
            or i.get("part_number") == "[UNREADABLE]"
        )
        if blank_pns > len(bom_items) * 0.2:
            confidence -= 0.15
        confidence = max(0.1, min(1.0, confidence))

        result = {
            "column_mapping": column_mapping,
            "extraction_summary": {
                "drawing_types_found": drawing_types,
                "confidence": round(confidence, 2),
                "scope_gap_flags": [],
                "review_flags": all_flags,
                "total_bom_rows_on_drawing": row_count,
            },
            "quantities": quantities,
            "bom_line_items": bom_items,
            "_model_used": model,
            "_stop_reason": extraction.get("_stop_reason", "end_turn"),
            "_output_tokens": total_tokens,
        }
        if was_truncated:
            result["_truncated"] = True

        print(
            f"SCAN [{filename}]: Complete -- {len(bom_items)} items, "
            f"confidence={confidence:.0%}, tokens={total_tokens}, "
            f"flags={len(all_flags)}",
            flush=True,
        )
        return result

    except Exception as e:
        err_str = str(e)
        print(f"SCAN [{filename}] FATAL ERROR: {err_str}", flush=True)

        # Rate limit final fallback
        if "429" in err_str or "rate_limit" in err_str.lower() or "overloaded" in err_str.lower():
            return {
                "error": "rate_limit",
                "error_message": "API rate limit reached. Please wait 60 seconds and try again.",
                "quantities": {}, "bom_line_items": [],
                "extraction_summary": {"confidence": 0, "scope_gap_flags": ["rate_limit"]},
            }

        import traceback
        err_detail = traceback.format_exc()
        print("SCAN ERROR DETAIL:", err_detail, flush=True)
        return {
            "error": str(e), "error_detail": err_detail,
            "quantities": {}, "bom_line_items": [],
            "extraction_summary": {"confidence": 0},
        }
