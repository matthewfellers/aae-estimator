"""
AAE Scanner Pipeline v2.5 — Multi-Stage BOM Extraction
=======================================================
5-stage pipeline with hardened anti-hallucination measures:
  Stage 1: Detect BOM table structure & column headers (full PDF)
  Stage 2: Extract every BOM row (BOM PAGE IMAGE — rendered via PyMuPDF)
  Stage 3: Derive estimator quantity buckets from BOM data
  Stage 4: Validate extraction (row count, column swap detection)
  Stage 5: AI-powered part number cross-verification (BOM PAGE IMAGE)

v2.5 — HIGH-RES IMAGE RENDERING FIX:
  After Stage 1 identifies BOM page(s), we:
  1. Extract those pages with pypdf (smaller PDF)
  2. Render to 300 DPI PNG image(s) with PyMuPDF
  3. Send the IMAGE to Claude (Stages 2 & 5), not the PDF
  This bypasses all PDF font/resource issues that caused hallucinated
  part numbers in v2.3/v2.4. Claude sees exactly what a human would see.
"""

import json, re, time, base64, io
from collections import Counter

# ---------------------------------------------------------------------------
# PDF Page Extraction — THE FIX for multi-page drawings
# ---------------------------------------------------------------------------
def _extract_pages(pdf_b64, page_numbers):
    """Extract specific pages from a PDF and return (new_pdf_b64, raw_text).

    Args:
        pdf_b64: Base64-encoded PDF (full drawing, e.g. 35 pages)
        page_numbers: List of 1-based page numbers to extract (e.g. [2] or [2,3])

    Returns:
        Tuple of (extracted_pdf_b64, raw_text_string).
        - extracted_pdf_b64: Base64-encoded PDF containing ONLY the requested pages.
        - raw_text_string: Text extracted directly from the PDF text layer.
          This is the EXACT character data from the PDF — no OCR needed.
          Empty string if text extraction fails.
        If page extraction fails, returns (original_pdf_b64, "") as fallback.
    """
    if not page_numbers:
        print("  [PageExtract] No page numbers specified, using full PDF", flush=True)
        return pdf_b64, ""

    try:
        from pypdf import PdfReader, PdfWriter

        # Decode the base64 PDF into bytes
        pdf_bytes = base64.b64decode(pdf_b64)
        reader = PdfReader(io.BytesIO(pdf_bytes))
        total_pages = len(reader.pages)

        print(f"  [PageExtract] Full PDF has {total_pages} pages, "
              f"extracting page(s) {page_numbers}", flush=True)

        writer = PdfWriter()
        pages_added = 0
        raw_text_parts = []
        for pg in page_numbers:
            idx = pg - 1  # Convert 1-based to 0-based index
            if 0 <= idx < total_pages:
                writer.add_page(reader.pages[idx])
                pages_added += 1
                # Extract raw text from the PDF text layer
                try:
                    page_text = reader.pages[idx].extract_text() or ""
                    if page_text.strip():
                        raw_text_parts.append(page_text)
                except Exception as txt_err:
                    print(f"  [PageExtract] Text extraction failed for page {pg}: {txt_err}",
                          flush=True)
            else:
                print(f"  [PageExtract] WARNING: Page {pg} out of range "
                      f"(PDF has {total_pages} pages)", flush=True)

        if pages_added == 0:
            print("  [PageExtract] No valid pages extracted, using full PDF", flush=True)
            return pdf_b64, ""

        raw_text = "\n".join(raw_text_parts)

        # Write the extracted pages to a new PDF in memory
        out_buf = io.BytesIO()
        writer.write(out_buf)
        out_buf.seek(0)
        extracted_b64 = base64.b64encode(out_buf.read()).decode("ascii")

        # Log size reduction
        original_kb = len(pdf_b64) * 3 / 4 / 1024  # approx decoded size
        extracted_kb = len(extracted_b64) * 3 / 4 / 1024
        reduction = (1 - extracted_kb / original_kb) * 100 if original_kb > 0 else 0

        print(f"  [PageExtract] Extracted {pages_added} page(s): "
              f"{original_kb:.0f}KB -> {extracted_kb:.0f}KB "
              f"({reduction:.0f}% smaller)", flush=True)
        if raw_text.strip():
            print(f"  [PageExtract] Raw text extracted: {len(raw_text)} chars "
                  f"(first 200: {raw_text[:200]!r})", flush=True)
        else:
            print("  [PageExtract] WARNING: No text layer found in PDF — "
                  "relying on vision only", flush=True)

        return extracted_b64, raw_text

    except ImportError:
        print("  [PageExtract] pypdf not installed, using full PDF", flush=True)
        return pdf_b64, ""
    except Exception as exc:
        print(f"  [PageExtract] ERROR: {exc}, using full PDF as fallback", flush=True)
        return pdf_b64, ""


# ---------------------------------------------------------------------------
# PDF-to-Image Rendering — THE v2.5 FIX for hallucinated part numbers
# ---------------------------------------------------------------------------
def _render_pdf_to_image(pdf_b64, dpi=300):
    """Render each page of a PDF to a high-resolution PNG image.

    This is the key fix: CAD-generated PDFs store BOM text as vector graphics,
    not as searchable text. When pypdf extracts pages, font resources can be
    lost, causing Claude to hallucinate characters. By rendering to a bitmap
    image at 300 DPI, Claude sees EXACTLY what a human would see — no font
    issues, no resource dependencies.

    Args:
        pdf_b64: Base64-encoded PDF (typically the extracted BOM page(s))
        dpi: Rendering resolution (300 = high quality for small CAD text)

    Returns:
        List of base64-encoded PNG strings (one per page).
        Empty list on failure (caller should fall back to PDF).
    """
    try:
        import fitz  # PyMuPDF

        pdf_bytes = base64.b64decode(pdf_b64)
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")

        images_b64 = []
        zoom = dpi / 72  # PDF default resolution is 72 DPI
        mat = fitz.Matrix(zoom, zoom)

        for page_num in range(len(doc)):
            page = doc[page_num]
            pix = page.get_pixmap(matrix=mat, alpha=False)
            png_bytes = pix.tobytes("png")
            img_b64 = base64.b64encode(png_bytes).decode("ascii")
            images_b64.append(img_b64)

            print(f"  [Render] Page {page_num+1}: {pix.width}x{pix.height} @ {dpi}DPI, "
                  f"{len(png_bytes)/1024:.0f}KB PNG", flush=True)

        doc.close()

        total_kb = sum(len(img) * 3 / 4 / 1024 for img in images_b64)
        print(f"  [Render] Total: {len(images_b64)} page(s), "
              f"{total_kb:.0f}KB image data", flush=True)
        return images_b64

    except ImportError:
        print("  [Render] PyMuPDF (fitz) not installed — cannot render PDF to image",
              flush=True)
        return []
    except Exception as exc:
        print(f"  [Render] ERROR rendering PDF to image: {exc}", flush=True)
        return []


# System-level instruction that forces JSON-only output.
_SYSTEM_JSON = (
    "You are a JSON-only API endpoint. Your ENTIRE response must be a single "
    "valid JSON object. Do NOT include any text before or after the JSON. "
    "Do NOT wrap the JSON in markdown code fences. Do NOT add explanations, "
    "commentary, or notes outside the JSON object. Start your response with "
    "the opening brace { and end with the closing brace }."
)


def _call_claude(claude_client, pdf_b64, prompt, model="claude-sonnet-4-20250514",
                 thinking_budget=16000, max_tokens=16000, stage_label="",
                 images_b64=None):
    """Shared helper: call Claude with a PDF or images + text prompt, return parsed JSON.

    If images_b64 is provided (list of base64 PNG strings), sends high-res images
    instead of the PDF document. This bypasses PDF font/resource issues that cause
    hallucinated part numbers in CAD-generated drawings.
    """

    t0 = time.time()

    # Build content blocks — images or PDF
    if images_b64:
        # v2.5: Send rendered PNG images instead of PDF
        content_blocks = []
        for img_b64 in images_b64:
            content_blocks.append({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": "image/png",
                    "data": img_b64
                }
            })
        content_blocks.append({"type": "text", "text": prompt})
        print(f"  [{stage_label}] Sending {len(images_b64)} image(s) to Claude",
              flush=True)
    else:
        # Original: send PDF document
        content_blocks = [
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

    api_kwargs = {
        "model": model,
        "max_tokens": max_tokens,
        "system": _SYSTEM_JSON,
        "messages": [{
            "role": "user",
            "content": content_blocks
        }]
    }

    if "sonnet" in model and thinking_budget > 0:
        if max_tokens <= thinking_budget:
            max_tokens = thinking_budget + max(8000, max_tokens)
            api_kwargs["max_tokens"] = max_tokens
        api_kwargs["thinking"] = {"type": "enabled", "budget_tokens": thinking_budget}
    elif "haiku" in model:
        api_kwargs["temperature"] = 0

    with claude_client.messages.stream(**api_kwargs) as stream:
        response = stream.get_final_message()

    elapsed = time.time() - t0
    stop_reason = response.stop_reason
    tokens_used = response.usage.output_tokens if response.usage else 0

    # Extract text block (skip thinking blocks safely)
    raw = ""
    for block in response.content:
        if block.type == "text":
            raw = block.text.strip()
            break
    if not raw:
        first = response.content[0] if response.content else None
        if first:
            raw = (getattr(first, "text", None) or
                   getattr(first, "thinking", None) or "")
            raw = raw.strip()

    if not raw:
        raise ValueError(f"Claude returned empty response [{stage_label}]")

    print(f"  [{stage_label}] API call: {elapsed:.1f}s, {tokens_used} tokens, "
          f"stop={stop_reason}, len={len(raw)}", flush=True)

    # ── Robust JSON extraction ──
    raw = re.sub(r"^```(?:json)?\s*\n?", "", raw)
    raw = re.sub(r"\n?\s*```\s*$", "", raw)
    raw = raw.strip()

    # Step 1: direct parse
    try:
        result = json.loads(raw)
    except json.JSONDecodeError:
        # Step 2: find JSON object in text (strip preamble)
        first_brace = raw.find('{')
        last_brace = raw.rfind('}')

        if first_brace >= 0 and last_brace > first_brace:
            json_str = raw[first_brace:last_brace + 1]
            if first_brace > 0:
                print(f"  [{stage_label}] Stripped {first_brace} chars of preamble",
                      flush=True)
            try:
                result = json.loads(json_str)
            except json.JSONDecodeError:
                # Step 3: bracket repair for truncated JSON
                opens_b = json_str.count('{') - json_str.count('}')
                opens_sq = json_str.count('[') - json_str.count(']')
                repaired = json_str
                if opens_sq > 0:
                    repaired += ']' * opens_sq
                if opens_b > 0:
                    repaired += '}' * opens_b
                try:
                    result = json.loads(repaired)
                    result["_truncated"] = True
                    print(f"  [{stage_label}] JSON repaired "
                          f"(+{opens_b} braces, +{opens_sq} brackets)", flush=True)
                except json.JSONDecodeError:
                    preview = raw[:300].replace('\n', '\\n')
                    print(f"  [{stage_label}] JSON FAILED: {preview}", flush=True)
                    raise ValueError(
                        f"Could not parse JSON [{stage_label}] (len={len(raw)}). "
                        f"First 200 chars: {raw[:200]}"
                    )
        else:
            preview = raw[:300].replace('\n', '\\n')
            print(f"  [{stage_label}] No JSON in response: {preview}", flush=True)
            raise ValueError(
                f"No JSON object in response [{stage_label}] (len={len(raw)}). "
                f"First 200 chars: {raw[:200]}"
            )

    result["_stop_reason"] = stop_reason
    result["_output_tokens"] = tokens_used
    if stop_reason == "max_tokens":
        result["_truncated"] = True

    return result


# ---------------------------------------------------------------------------
# Stage 1: Detect BOM table structure
# ---------------------------------------------------------------------------
def _stage1_detect_structure(claude_client, pdf_b64):
    prompt = (
        "You are reading an industrial electrical panel drawing PDF.\n"
        "Your ONLY job is to find the BOM (Bill of Materials) table and report its structure.\n\n"
        "DO NOT extract any row data yet. Just report the table structure.\n\n"
        "Look through every page. Find any table that lists parts/components with quantities.\n\n"
        "Return this JSON:\n"
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
        "Count CAREFULLY -- go row by row and count each one.\n\n"
        "CRITICAL: Report the EXACT page number(s) where the BOM table appears in pages_with_bom.\n"
        "This drawing may have 30+ pages of schematics, wiring diagrams, etc.\n"
        "The BOM table is usually titled 'BILL OF MATERIALS' or 'BOM' — identify those pages.\n"
        "Do NOT include pages with wire schedules, terminal schedules, nameplate schedules, or schematics."
    )

    result = _call_claude(claude_client, pdf_b64, prompt,
                          thinking_budget=10000, max_tokens=4000,
                          stage_label="Stage1")
    print(f"SCAN Stage 1: Found {result.get('bom_tables_found', 0)} BOM table(s), "
          f"{result.get('total_bom_rows', '?')} rows, "
          f"headers={result.get('column_headers_left_to_right', [])}", flush=True)
    return result


# ---------------------------------------------------------------------------
# Stage 2: Extract every BOM row — HARDENED ANTI-HALLUCINATION
# ---------------------------------------------------------------------------
def _stage2_extract_bom(claude_client, pdf_b64, structure, bom_images=None):
    """Pure transcription of every BOM row. This is the critical stage
    where accuracy matters most. Uses double-read strategy and explicit
    anti-hallucination guardrails.

    If bom_images is provided, sends high-res PNG images to Claude instead
    of the PDF — this fixes hallucination from PDF font/resource issues."""

    col_map = structure.get("column_mapping", {})
    headers = structure.get("column_headers_left_to_right", [])
    total_rows = structure.get("total_bom_rows", 0)
    has_mfg = structure.get("has_manufacturer_column", True)
    has_desc = structure.get("has_description_column", True)
    bom_pages = structure.get("pages_with_bom", [])

    col_info = f"The column headers from left to right are: {headers}"
    mapping_lines = []
    for k, v in col_map.items():
        if v:
            mapping_lines.append(f'  - Column "{v}" -> field "{k}"')
    mapping_info = "\n".join(mapping_lines)

    # Critical: tell Claude exactly what it's looking at
    bom_extracted = structure.get("_bom_extracted", False)
    using_images = bom_images is not None and len(bom_images) > 0
    if bom_extracted:
        if using_images:
            page_instruction = (
                "THIS IMAGE SHOWS ONLY THE BOM (BILL OF MATERIALS) TABLE.\n"
                "All schematics, wiring diagrams, and other pages have been removed.\n"
                "This is a high-resolution rendering of the BOM page from the drawing.\n"
                "Read EVERY row visible in this image. The entire image is your BOM source.\n\n"
            )
        else:
            page_instruction = (
                "THIS PDF CONTAINS ONLY THE BOM TABLE PAGE(S).\n"
                "All schematics, wiring diagrams, and other pages have been removed.\n"
                "Read EVERY page in this PDF — every page is part of the BOM table.\n"
                "Do NOT skip any page. The entire document is your BOM source.\n\n"
            )
    elif bom_pages:
        page_instruction = (
            f"IMPORTANT: The BOM table is on page(s) {bom_pages} of this PDF.\n"
            f"IGNORE ALL OTHER PAGES. Only read the BOM table on page(s) {bom_pages}.\n"
            "Do NOT read data from schematics, wiring diagrams, nameplate schedules, "
            "terminal schedules, or any other tables in this drawing.\n"
            "ONLY read the BILL OF MATERIALS table.\n\n"
        )
    else:
        page_instruction = ""

    # If we have raw text extracted from the PDF text layer, include it
    # as the PRIMARY data source — this is the exact character data
    raw_text = structure.get("_bom_raw_text", "")
    if raw_text.strip():
        text_section = (
            "=== RAW TEXT EXTRACTED FROM PDF (PRIMARY SOURCE — EXACT CHARACTERS) ===\n"
            "The following text was extracted directly from the PDF file's text layer.\n"
            "These are the EXACT characters embedded in the PDF — not OCR, not guessed.\n"
            "USE THIS TEXT as your PRIMARY source for part numbers, descriptions, and all data.\n"
            "Use the PDF image only to understand the TABLE STRUCTURE (rows, columns, layout).\n"
            "When the raw text and your visual reading disagree, the RAW TEXT WINS.\n\n"
            f"{raw_text}\n\n"
            "=== END RAW TEXT ===\n\n"
        )
    else:
        text_section = ""

    prompt = (
        "You are transcribing the BOM (Bill of Materials) table from an electrical panel drawing.\n\n"
        f"{page_instruction}"
        f"{text_section}"
        f"COLUMN STRUCTURE (already identified):\n{col_info}\n\n"
        f"FIELD MAPPING:\n{mapping_info}\n\n"
        f"Has manufacturer column: {has_mfg}\n"
        f"Has description column: {has_desc}\n"
        f"Expected row count: {total_rows}\n\n"

        "YOUR TASK: Read EVERY row of the BOM table. Copy each cell value EXACTLY as printed.\n\n"

        "=== CRITICAL RULES ===\n\n"

        "RULE 1 — ONE ROW = ONE ITEM:\n"
        "If the QTY column says 32, output qty:32 as ONE item.\n"
        "Do NOT create 32 separate items. That is wrong.\n\n"

        "RULE 2 — PART NUMBERS ARE SACRED:\n"
        "These part numbers will be used to ORDER real parts. A wrong character = wrong part delivered.\n"
        "Copy EVERY character EXACTLY: dashes, slashes, letters, numbers, spaces.\n"
    )

    # Add extra instructions depending on whether we have raw text
    if raw_text.strip():
        prompt += (
            "  The RAW TEXT above contains the exact part numbers from the PDF.\n"
            "  Cross-reference EVERY part number against the raw text.\n"
            "  If a part number appears in the raw text, use the EXACT string from the raw text.\n"
            "  Do NOT modify, correct, or 'improve' part numbers from the raw text.\n\n"
        )
    else:
        prompt += (
            "  DOUBLE-READ STRATEGY: For each part number:\n"
            "    a) Read it LEFT to RIGHT. Write it down.\n"
            "    b) Read it RIGHT to LEFT, character by character. Compare.\n"
            "    c) Count the total characters. Does your extraction have the same count?\n"
            "    d) If the PN has dashes (e.g., 2090-CSBM1DG-14LN03), verify each segment between dashes independently.\n"
            "  CONFUSABLE CHARACTERS — pay extra attention to:\n"
            "    O (letter) vs 0 (zero), l (lowercase L) vs 1 (one), I (letter I) vs 1 (one),\n"
            "    B vs 8, S vs 5, G vs 6, Z vs 2, D vs 0\n"
            '  If you cannot read even ONE character clearly, write "[UNREADABLE]" for the whole part number.\n'
            "  NEVER fill in a character from your training knowledge. Read it from the PDF or mark it unreadable.\n\n"
        )

    prompt += (
        "RULE 3 — ANTI-HALLUCINATION:\n"
        "  You have been trained on millions of part numbers. That is DANGEROUS here.\n"
        "  Your memory of what part numbers SHOULD look like can override what you actually SEE.\n"
        "  For EVERY part number you write, ask yourself:\n"
        '    "Did I read this specific string from the PDF image, or am I writing what I think it should be?"\n'
        "  If you are writing from memory rather than reading, STOP. Go back to the PDF cell and re-read.\n"
        "  The drawing is the ONLY source of truth. Your training data is NOT a source of truth.\n\n"

        "RULE 4 — COLUMN MAPPING:\n"
        '  If there is no manufacturer column, leave manufacturer as "".\n'
        '  If there is no description column, leave description as "".\n'
        "  SELF-CHECK each row:\n"
        "  - Is description a long spec string? (Good) Or 1-2 words like a company name? (Wrong — you swapped columns)\n"
        "  - Is manufacturer a short company name? (Good) Or a long spec string? (Wrong — you swapped columns)\n\n"

        "RULE 5 — COMPLETENESS:\n"
        "  Do NOT skip ANY rows. Every physical row in the table = one item in your output.\n"
        '  For qty values like "A/R", "AR", "REF", use qty:1 and put the original text in notes.\n\n'

        "Return this JSON:\n"
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
        "If it does not match, you missed rows — go back and find them."
    )

    think_budget = max(32000, total_rows * 600)
    think_budget = min(think_budget, 100000)
    max_out = max(20000, total_rows * 350)
    max_out = min(max_out, 128000)

    print(f"SCAN Stage 2: Extracting {total_rows} rows "
          f"(thinking={think_budget}, max_tokens={max_out}, "
          f"images={'yes' if bom_images else 'no'})", flush=True)
    result = _call_claude(claude_client, pdf_b64, prompt,
                          thinking_budget=think_budget, max_tokens=max_out,
                          stage_label="Stage2", images_b64=bom_images)

    items = result.get("bom_line_items", [])
    rows_reported = result.get("rows_extracted", len(items))
    print(f"SCAN Stage 2: Got {len(items)} items (model reported {rows_reported})", flush=True)

    return result


# ---------------------------------------------------------------------------
# Stage 3: Derive estimator quantities from BOM
# ---------------------------------------------------------------------------
def _stage3_derive_quantities(claude_client, pdf_b64, bom_items):
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
        "Return this JSON:\n"
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
                          thinking_budget=12000, max_tokens=8000,
                          stage_label="Stage3")
    print(f"SCAN Stage 3: Quantities derived", flush=True)
    return result


# ---------------------------------------------------------------------------
# Stage 4: Validate extraction (deterministic, no AI)
# ---------------------------------------------------------------------------
def _stage4_validate(structure, bom_items):
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
        "lapp", "automationdirect",
    }
    swap_count = 0
    for item in bom_items:
        desc = (item.get("description") or "").strip().lower()
        mfr = (item.get("manufacturer") or "").strip().lower()
        if desc and len(desc.split()) <= 3 and any(m in desc for m in known_mfrs):
            swap_count += 1
        if mfr and len(mfr.split()) > 5:
            swap_count += 1

    if swap_count > 0:
        if swap_count > len(bom_items) * 0.3:
            flags.append(
                f"COLUMN SWAP DETECTED: {swap_count} items appear to have "
                "manufacturer/description swapped. Auto-correcting.")
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

    # Check 3: Duplicate part numbers
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

    # Check 5: All qty=1
    if len(bom_items) > 5:
        try:
            all_qty_1 = all(int(item.get("qty", 1)) == 1 for item in bom_items)
        except (ValueError, TypeError):
            all_qty_1 = False
        if all_qty_1:
            flags.append(
                "All items have qty=1 -- verify the QTY column was read correctly")

    return flags


# ---------------------------------------------------------------------------
# Stage 5: AI-powered part number verification
# ---------------------------------------------------------------------------
def _stage5_verify_part_numbers(claude_client, pdf_b64, bom_items,
                                bom_pages=None, bom_extracted=False,
                                bom_raw_text="", bom_images=None):
    pn_lines = []
    for item in bom_items:
        pn = item.get("part_number", "")
        item_num = item.get("item_num", "?")
        if pn and pn != "[UNREADABLE]":
            pn_lines.append(f"  Item {item_num}: {pn}")
    pn_list = "\n".join(pn_lines)

    using_images = bom_images is not None and len(bom_images) > 0
    page_note = ""
    if bom_extracted:
        if using_images:
            page_note = (
                "This high-resolution IMAGE shows ONLY the BOM table from the drawing — "
                "all other pages have been removed. Verify each part number against "
                "what you see in this image, character by character.\n\n"
            )
        else:
            page_note = (
                "This PDF contains ONLY the BOM table page(s) — all other pages "
                "have been removed. Verify each part number against what you see "
                "on every page of this PDF.\n\n"
            )
    elif bom_pages:
        page_note = (
            f"The BOM table is on page(s) {bom_pages}. "
            "Look ONLY at the BOM table on those pages to verify — "
            "ignore all other pages and tables.\n\n"
        )

    # Include raw text if available — this is the definitive source
    raw_text_section = ""
    if bom_raw_text.strip():
        raw_text_section = (
            "=== RAW TEXT FROM PDF (DEFINITIVE SOURCE) ===\n"
            "The following text was extracted directly from the PDF's text layer.\n"
            "These are the EXACT characters. Use this to verify part numbers.\n\n"
            f"{bom_raw_text}\n\n"
            "=== END RAW TEXT ===\n\n"
        )

    prompt = (
        "I extracted these part numbers from the BOM table in this drawing PDF.\n"
        f"{page_note}"
        f"{raw_text_section}"
        "Your job: verify each part number CHARACTER BY CHARACTER against the PDF.\n\n"
        "EXTRACTED PART NUMBERS:\n"
        f"{pn_list}\n\n"
        "For EACH part number above:\n"
        "1. Find it in the actual BOM table in the PDF"
    )
    if bom_raw_text.strip():
        prompt += " AND in the raw text above"
    prompt += (
        "\n"
        "2. Compare character by character — read left to right, then right to left\n"
        "3. Check for commonly confused characters: O vs 0, l vs 1, B vs 8, S vs 5\n"
        "4. Count the characters — does the extracted version have the same count as the PDF?\n"
        "5. If it matches exactly, mark it verified\n"
        "6. If ANY character is wrong, provide the CORRECT value from the PDF\n"
        "7. If you cannot find it in the PDF at all, mark it as not_found\n\n"
        "Return this JSON:\n"
        "{\n"
        '  "verifications": [\n'
        "    {\n"
        '      "item_num": 1,\n'
        '      "extracted": "the-part-number",\n'
        '      "status": "verified",\n'
        '      "corrected": "",\n'
        '      "note": ""\n'
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
        stage_label="Stage5",
        images_b64=bom_images,
    )

    verifications = result.get("verifications", [])
    corrected_count = sum(1 for v in verifications if v.get("status") == "corrected")
    not_found_count = sum(1 for v in verifications if v.get("status") == "not_found")
    verified_count = sum(1 for v in verifications if v.get("status") == "verified")

    print(
        f"SCAN Stage 5: {verified_count} verified, {corrected_count} corrected, "
        f"{not_found_count} not found",
        flush=True,
    )

    return result


def _apply_corrections(bom_items, verification_result):
    """Apply corrections from Stage 5 AND tag every item with _verification_status."""

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
    verified_count = 0

    for item in bom_items:
        item_num = item.get("item_num")
        pn = (item.get("part_number") or "").strip()

        # Items with no PN or UNREADABLE get special status
        if not pn or pn == "[UNREADABLE]":
            item["_verification_status"] = "no_pn"
            continue

        if item_num in corrections:
            v = corrections[item_num]
            status = v.get("status", "")

            if status == "corrected" and v.get("corrected"):
                old_pn = item.get("part_number", "")
                new_pn = v["corrected"]
                item["part_number"] = new_pn
                item["_verification_status"] = "corrected"
                item["_original_pn"] = old_pn
                item["notes"] = (
                    (item.get("notes", "") +
                     f" [PN corrected: {old_pn} -> {new_pn}]").strip()
                )
                corrected_count += 1

            elif status == "not_found":
                item["_verification_status"] = "not_found"
                item["notes"] = (
                    (item.get("notes", "") +
                     " [WARNING: PN not verified in drawing]").strip()
                )
                not_found_count += 1

            elif status == "verified":
                item["_verification_status"] = "verified"
                verified_count += 1

            else:
                item["_verification_status"] = "unverified"
        else:
            # Stage 5 didn't return a result for this item
            item["_verification_status"] = "unverified"

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

    summary = {
        "verified": verified_count,
        "corrected": corrected_count,
        "not_found": not_found_count,
        "unverified": sum(1 for i in bom_items if i.get("_verification_status") == "unverified"),
        "no_pn": sum(1 for i in bom_items if i.get("_verification_status") == "no_pn"),
    }

    return bom_items, flags, summary


# ---------------------------------------------------------------------------
# Main entry point: scan_drawing()
# ---------------------------------------------------------------------------
def scan_drawing(claude_client, pdf_b64, filename="drawing.pdf"):
    """Multi-stage BOM extraction pipeline."""

    model = "claude-sonnet-4-20250514"
    total_tokens = 0
    was_truncated = False
    all_flags = []
    verification_summary = {}
    pipeline_start = time.time()

    try:
        # == STAGE 1 ========================================================
        t1 = time.time()
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
                print(f"SCAN: Rate limit on Stage 1, waiting 30s...", flush=True)
                time.sleep(30)
                try:
                    structure = _stage1_detect_structure(claude_client, pdf_b64)
                except Exception:
                    raise e1
            else:
                raise

        total_tokens += structure.get("_output_tokens", 0)
        print(f"SCAN Stage 1 done in {time.time()-t1:.1f}s", flush=True)

        bom_count = structure.get("bom_tables_found", 0)
        row_count = structure.get("total_bom_rows", 0)
        drawing_types = structure.get("drawing_types_found", [])

        if bom_count == 0 or row_count == 0:
            print(f"SCAN: No BOM table found -- quantity-only scan", flush=True)
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

        # == EXTRACT BOM PAGES (THE KEY FIX) ==================================
        # Instead of sending the full 35-page drawing to Stage 2 & 5, extract
        # ONLY the BOM page(s) into a small PDF. This eliminates the noise from
        # schematics, wiring diagrams, etc. that was causing hallucination.
        bom_pages = structure.get("pages_with_bom", [])
        bom_extracted = False
        bom_raw_text = ""
        bom_images = None  # v2.5: rendered PNG images for Stages 2 & 5
        if bom_pages:
            bom_pdf_b64, bom_raw_text = _extract_pages(pdf_b64, bom_pages)
            # Check if extraction actually produced a different (smaller) PDF
            bom_extracted = (bom_pdf_b64 != pdf_b64)
            if bom_extracted:
                # Signal to Stage 2 that page numbers no longer apply —
                # the extracted PDF contains ONLY BOM pages starting at page 1
                structure["_bom_extracted"] = True
                structure["_bom_raw_text"] = bom_raw_text
                print(f"SCAN [{filename}]: BOM pages extracted successfully "
                      f"(raw text: {len(bom_raw_text)} chars)", flush=True)

                # v2.5: Render BOM pages to high-res PNG images
                # This is THE FIX — bypasses all PDF font/resource issues
                bom_images = _render_pdf_to_image(bom_pdf_b64, dpi=300)
                if bom_images:
                    print(f"SCAN [{filename}]: Rendered {len(bom_images)} BOM page(s) "
                          f"to 300 DPI PNG — Stage 2 & 5 will use IMAGES", flush=True)
                else:
                    print(f"SCAN [{filename}]: Image rendering failed, "
                          f"falling back to extracted PDF", flush=True)
        else:
            print(f"SCAN [{filename}]: No BOM pages identified, using full PDF", flush=True)
            bom_pdf_b64 = pdf_b64

        # == STAGE 2 ========================================================
        # Uses BOM-ONLY PDF (not full drawing)
        t2 = time.time()
        input_mode = "IMAGES" if bom_images else ("BOM PDF" if bom_extracted else "full PDF")
        print(f"SCAN [{filename}]: Starting Stage 2 -- extracting {row_count} rows "
              f"(mode: {input_mode}, pages: {bom_pages})", flush=True)
        try:
            extraction = _stage2_extract_bom(claude_client, bom_pdf_b64, structure,
                                             bom_images=bom_images)
        except Exception as e2:
            err_str = str(e2)
            if "429" in err_str or "rate_limit" in err_str.lower() or "overloaded" in err_str.lower():
                print(f"SCAN: Rate limit on Stage 2, waiting 30s...", flush=True)
                time.sleep(30)
                try:
                    extraction = _stage2_extract_bom(claude_client, bom_pdf_b64, structure,
                                                     bom_images=bom_images)
                except Exception:
                    raise e2
            else:
                raise

        total_tokens += extraction.get("_output_tokens", 0)
        was_truncated = was_truncated or extraction.get("_truncated", False)
        bom_items = extraction.get("bom_line_items", [])
        print(f"SCAN Stage 2 done in {time.time()-t2:.1f}s", flush=True)

        # == STAGE 4 (deterministic) ========================================
        print(f"SCAN [{filename}]: Stage 4 -- validation", flush=True)
        validation_flags = _stage4_validate(structure, bom_items)
        all_flags.extend(validation_flags)

        # == STAGE 5 ========================================================
        # Uses BOM-ONLY PDF (not full drawing) — same extracted pages as Stage 2
        t5 = time.time()
        print(f"SCAN [{filename}]: Starting Stage 5 -- PN verification "
              f"(mode: {input_mode}, pages: {bom_pages})", flush=True)
        try:
            verify_result = _stage5_verify_part_numbers(
                claude_client, bom_pdf_b64, bom_items,
                bom_pages=bom_pages, bom_extracted=bom_extracted,
                bom_raw_text=bom_raw_text, bom_images=bom_images
            )
            total_tokens += verify_result.get("_output_tokens", 0)
            bom_items, correction_flags, verification_summary = _apply_corrections(
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
                        claude_client, bom_pdf_b64, bom_items,
                        bom_pages=bom_pages, bom_extracted=bom_extracted,
                        bom_raw_text=bom_raw_text, bom_images=bom_images
                    )
                    total_tokens += verify_result.get("_output_tokens", 0)
                    bom_items, correction_flags, verification_summary = _apply_corrections(
                        bom_items, verify_result
                    )
                    all_flags.extend(correction_flags)
                except Exception:
                    print(f"SCAN: Stage 5 failed, PNs unverified", flush=True)
                    all_flags.append(
                        "Part number verification skipped due to rate limit -- "
                        "review all part numbers manually"
                    )
                    for item in bom_items:
                        item["_verification_status"] = "unverified"
            else:
                print(f"SCAN: Stage 5 error: {err_str}, continuing unverified", flush=True)
                all_flags.append(
                    "Part number verification failed -- review all part numbers manually"
                )
                for item in bom_items:
                    item["_verification_status"] = "unverified"
        print(f"SCAN Stage 5 done in {time.time()-t5:.1f}s", flush=True)

        # == STAGE 3 ========================================================
        t3 = time.time()
        print(f"SCAN [{filename}]: Starting Stage 3 -- quantities", flush=True)
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
                    print(f"SCAN: Stage 3 failed, BOM without quantities", flush=True)
                    qty_result = {"quantities": {}}
            else:
                print(f"SCAN: Stage 3 error: {err_str}, continuing", flush=True)
                qty_result = {"quantities": {}}

        total_tokens += qty_result.get("_output_tokens", 0)
        quantities = qty_result.get("quantities", {})
        print(f"SCAN Stage 3 done in {time.time()-t3:.1f}s", flush=True)

        # Apply categories
        cat_map = {}
        for ca in qty_result.get("category_assignments", []):
            cat_map[ca.get("item_num")] = ca.get("category", "Other")
        for item in bom_items:
            if item.get("item_num") in cat_map:
                item["category"] = cat_map[item["item_num"]]
            elif "category" not in item or not item["category"]:
                item["category"] = "Other"

        # == Assemble result ================================================
        col_map_raw = structure.get("column_mapping", {})
        detected_headers = structure.get("column_headers_left_to_right", [])
        column_mapping = {
            "detected_headers": detected_headers,
            "mapping": {v: k for k, v in col_map_raw.items() if v},
        }

        confidence = 0.9
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

        total_elapsed = time.time() - pipeline_start
        result = {
            "column_mapping": column_mapping,
            "extraction_summary": {
                "drawing_types_found": drawing_types,
                "confidence": round(confidence, 2),
                "scope_gap_flags": [],
                "review_flags": all_flags,
                "total_bom_rows_on_drawing": row_count,
                "verification_summary": verification_summary,
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
            f"SCAN [{filename}]: COMPLETE -- {len(bom_items)} items, "
            f"confidence={confidence:.0%}, tokens={total_tokens}, "
            f"flags={len(all_flags)}, time={total_elapsed:.1f}s",
            flush=True,
        )
        return result

    except Exception as e:
        err_str = str(e)
        print(f"SCAN [{filename}] FATAL: {err_str}", flush=True)

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
