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
# Programmatic BOM page detection — backup for Stage 1 misses
# ---------------------------------------------------------------------------
def _detect_bom_pages_programmatic(pdf_b64, hint_pages=None):
    """Scan the PDF for BOM-like tables.

    If hint_pages is provided (from Stage 1), only scans nearby pages (±3)
    to find continuation pages efficiently. If no hints, scans all pages.

    Returns a list of 1-based page numbers that likely contain BOM tables.
    Uses three strategies (each page tries all until one succeeds):
      1. find_tables(): pages with a table having >= MIN_BOM_ROWS rows
      2. Word-scan: pages containing BOM header keywords in the text layer
      3. OCR-scan (for SHX/vector-font drawings with no text layer):
         renders pages at 150 DPI, runs pytesseract, looks for keywords
    """
    MIN_BOM_ROWS = 8  # tables with fewer rows are title blocks, not BOMs
    BOM_KEYWORDS = [
        "BILL OF MATERIALS", "B.O.M.", "PARTS LIST",
        "MATERIAL LIST", "BOM CONT", "CONT FROM",
        "CONT ON SHT",
    ]

    try:
        import fitz
        pdf_bytes = base64.b64decode(pdf_b64)
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        total_pages = len(doc)
        bom_pages = []
        needs_ocr_scan = []  # pages where text-layer strategies failed

        # Determine which pages to scan.  If Stage 1 found BOM pages,
        # only check nearby pages (±3) for continuations — scanning all
        # 15+ pages with OCR adds ~30s and can cause timeouts.
        if hint_pages:
            scan_set = set()
            for hp in hint_pages:
                for offset in range(-3, 4):  # hp-3 … hp+3
                    pg = hp + offset
                    if 1 <= pg <= total_pages:
                        scan_set.add(pg)
            # Always include hint pages themselves
            scan_set.update(hint_pages)
            pages_to_scan = sorted(scan_set)
            print(f"  [BOM-detect] Scanning {len(pages_to_scan)} pages near "
                  f"Stage 1 hints {hint_pages}", flush=True)
        else:
            pages_to_scan = list(range(1, total_pages + 1))

        for pg_num in pages_to_scan:
            pg_idx = pg_num - 1
            page = doc[pg_idx]
            found = False

            # --- Strategy 1: find_tables() ---
            try:
                tabs = page.find_tables()
                if tabs.tables:
                    best = max(tabs.tables, key=lambda t: len(t.rows))
                    if len(best.rows) >= MIN_BOM_ROWS:
                        bom_pages.append(pg_num)
                        print(f"  [BOM-detect] Page {pg_num}: table with "
                              f"{len(best.rows)} rows (find_tables)", flush=True)
                        found = True
            except (AttributeError, Exception):
                pass

            # --- Strategy 2: keyword scan on text-layer words ---
            if not found:
                try:
                    words = page.get_text("words")
                    if words:
                        full_text = " ".join(w[4] for w in words).upper()
                        for kw in BOM_KEYWORDS:
                            if kw in full_text:
                                bom_pages.append(pg_num)
                                print(f"  [BOM-detect] Page {pg_num}: keyword "
                                      f"'{kw}' found (word-scan)", flush=True)
                                found = True
                                break
                except Exception:
                    pass

            if not found:
                needs_ocr_scan.append(pg_idx)

        # --- Strategy 3: OCR scan for SHX/vector-font drawings ---
        # Only runs if Strategies 1 & 2 found nothing on the scanned pages.
        if needs_ocr_scan and not bom_pages:
            try:
                import pytesseract
                from PIL import Image
                print(f"  [BOM-detect] Text-layer strategies found nothing — "
                      f"OCR scanning {len(needs_ocr_scan)} pages", flush=True)

                # Exact keywords for clean OCR at higher DPI
                BOM_KW_EXACT = list(BOM_KEYWORDS)
                # Fuzzy fragments for noisy SHX font OCR —
                # "BILL OF MATERIALS" often OCRs as "BLL OF WATERALS" etc.
                BOM_KW_FUZZY = [
                    "BILL OF", "OF MATERIALS", "PARTS LIST",
                    "MATERIAL LIST", "CONT FROM", "CONT ON",
                    "BLL OF", "WATERALS", "MATER",
                ]

                for pg_idx in needs_ocr_scan:
                    page = doc[pg_idx]
                    pg_num = pg_idx + 1

                    # Render at 150 DPI — balance of speed vs accuracy
                    zoom = 150 / 72
                    mat = fitz.Matrix(zoom, zoom)
                    pix = page.get_pixmap(matrix=mat, alpha=False)
                    img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)

                    # Quick OCR — PSM 11 (sparse text), no preprocessing
                    try:
                        ocr_text = pytesseract.image_to_string(
                            img, config="--psm 11 --oem 3"
                        ).upper()
                    except Exception:
                        continue
                    finally:
                        del img, pix  # free memory immediately

                    # Try exact keywords first
                    found_kw = False
                    for kw in BOM_KW_EXACT:
                        if kw in ocr_text:
                            bom_pages.append(pg_num)
                            print(f"  [BOM-detect] Page {pg_num}: keyword "
                                  f"'{kw}' found (OCR-scan)", flush=True)
                            found_kw = True
                            break

                    # Fuzzy fallback for garbled SHX font OCR
                    if not found_kw:
                        fuzzy_hits = sum(1 for kw in BOM_KW_FUZZY
                                         if kw in ocr_text)
                        if fuzzy_hits >= 2:
                            bom_pages.append(pg_num)
                            print(f"  [BOM-detect] Page {pg_num}: {fuzzy_hits} "
                                  f"fuzzy BOM keywords (OCR-scan)", flush=True)

            except ImportError:
                print("  [BOM-detect] pytesseract not available for OCR scan",
                      flush=True)
            except Exception as ocr_err:
                print(f"  [BOM-detect] OCR scan error: {ocr_err}", flush=True)

        doc.close()
        return sorted(set(bom_pages))

    except ImportError:
        print("  [BOM-detect] fitz not available, skipping", flush=True)
        return []
    except Exception as exc:
        print(f"  [BOM-detect] ERROR: {exc}", flush=True)
        return []


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
        return pdf_b64, "", "pypdf"

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
        text_source = "pypdf"  # may be upgraded to "columns" below

        # Try column-aware extraction first (fitz word bounding boxes).
        # This produces a pipe-separated table where qty and part number
        # are always in separate cells — column merging is impossible.
        col_text = _extract_bom_columns(pdf_bytes, page_numbers)
        if col_text.strip():
            raw_text_parts.append(col_text)
            text_source = "columns"
            print(f"  [PageExtract] column-aware: {len(col_text)} chars "
                  f"for pages {page_numbers}", flush=True)

        for pg in page_numbers:
            idx = pg - 1  # Convert 1-based to 0-based index
            if 0 <= idx < total_pages:
                writer.add_page(reader.pages[idx])
                pages_added += 1
                if not col_text.strip():
                    # Column-aware failed — fall back to pypdf plain mode.
                    # Do NOT use extraction_mode="layout": that mode pads
                    # columns with spaces and merges adjacent text
                    # (item# "4" + part "025411-10" → "1025411-10";
                    #  qty "14" + nearby "3" → "143").
                    page_text = ""
                    try:
                        page_text = reader.pages[idx].extract_text() or ""
                        if page_text.strip():
                            print(f"  [PageExtract] pypdf plain fallback: "
                                  f"{len(page_text)} chars from page {pg}",
                                  flush=True)
                    except Exception as txt_err:
                        print(f"  [PageExtract] Text extraction failed for "
                              f"page {pg}: {txt_err}", flush=True)
                    if page_text.strip():
                        raw_text_parts.append(page_text)
            else:
                print(f"  [PageExtract] WARNING: Page {pg} out of range "
                      f"(PDF has {total_pages} pages)", flush=True)

        if pages_added == 0:
            print("  [PageExtract] No valid pages extracted, using full PDF", flush=True)
            return pdf_b64, "", "pypdf"

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

        return extracted_b64, raw_text, text_source

    except ImportError:
        print("  [PageExtract] pypdf not installed, using full PDF", flush=True)
        return pdf_b64, "", "pypdf"
    except Exception as exc:
        print(f"  [PageExtract] ERROR: {exc}, using full PDF as fallback", flush=True)
        return pdf_b64, "", "pypdf"


# ---------------------------------------------------------------------------
# Horizontal BOM helper
# ---------------------------------------------------------------------------
def _try_horizontal_bom(raw_words, page_num):
    """Detect and extract a HORIZONTAL / SIDEWAYS BOM.

    Some CAD drawings lay the BOM sideways: each item is a COLUMN, and
    each field type (ITEM#, QTY, PART, MFG) is a horizontal BAND at a
    different y-position.  fitz extracts all item numbers as one long line
    at y≈36, all quantities at y≈59, all parts at y≈75, etc.

    Strategy:
      1. Group words into y-bands (20pt tolerance).
      2. Find the ITEM band: a band containing 5+ sequential positive integers.
      3. Find the QTY band: the band just below ITEM where words are
         mostly parseable as positive integers.
      4. Find the PART band: next band below QTY with alphanumeric content.
      5. For each item, find the QTY and PART word nearest in x-center.
         This handles noise (e.g. 'DATE' from the title block) naturally —
         noise words won't be near any item's x-center.
      6. Collect MFG from remaining bands using the same x-proximity rule.
      7. Output a vertical pipe-separated table (one row per item).

    Returns pipe-separated table string, or "" if not a horizontal BOM.
    """
    # Qty band (y≈59) and part band (y≈75) are only ~16pt apart in this drawing.
    # Tolerance must be well below 16pt so they stay in separate bands.
    # Within a single CAD text row, y variation is typically <2pt, so 6pt is safe.
    Y_BAND_TOL = 6     # pt — words within 6pt vertically are the same band

    # ── 1. Group into y-bands ───────────────────────────────────────────────
    bands = []   # list of {'y': float, 'words': list}
    for w in sorted(raw_words, key=lambda w: w[1]):
        if not w[4].strip():
            continue
        placed = False
        for band in bands:
            if abs(w[1] - band['y']) <= Y_BAND_TOL:
                band['words'].append(w)
                placed = True
                break
        if not placed:
            bands.append({'y': w[1], 'words': [w]})
    bands.sort(key=lambda b: b['y'])

    # ── 2. Find ITEM band ───────────────────────────────────────────────────
    # Must contain ≥5 distinct positive integers that form a mostly-sequential
    # set (gaps of ≤2 allowed, e.g. item 34 deleted from a 48-item BOM).
    item_band_idx = None
    item_xn = []   # [(x_center, item_number), …] sorted by x

    for bi, band in enumerate(bands):
        sw = sorted(band['words'], key=lambda w: w[0])
        int_words = []
        for w in sw:
            try:
                n = int(w[4].strip())
                if n > 0:
                    int_words.append(((w[0] + w[2]) / 2, n))
            except ValueError:
                pass
        if len(int_words) < 5:
            continue
        nums = sorted(set(n for _, n in int_words))
        max_n = nums[-1]
        # Allow up to 3 gaps in the sequence (deleted items are normal)
        gaps = sum(1 for i in range(len(nums) - 1) if nums[i + 1] - nums[i] > 1)
        if max_n >= 5 and gaps <= 3:
            item_band_idx = bi
            item_xn = sorted(int_words, key=lambda iw: iw[0])
            print(f"  [ColumnExtract] HorizBOM: item band y≈{band['y']:.1f}, "
                  f"{len(item_xn)} items (max={max_n}, gaps={gaps})", flush=True)
            break

    if item_band_idx is None:
        return ""   # Not a horizontal BOM

    # Estimate column width for x-proximity matching
    if len(item_xn) > 1:
        x_span   = item_xn[-1][0] - item_xn[0][0]
        col_half = (x_span / (len(item_xn) - 1)) * 0.55
    else:
        col_half = 20

    def nearest_by_x(candidates, item_x):
        """Return text of candidate word whose x-center is closest to item_x."""
        best_text, best_dist = "", float('inf')
        for cx, text in candidates:
            d = abs(cx - item_x)
            if d < best_dist:
                best_dist, best_text = d, text
        return best_text if best_dist <= col_half else ""

    # ── 3. Find QTY band ────────────────────────────────────────────────────
    qty_band_idx = None
    qty_xv = []   # [(x_center, qty_string)]

    for bi in range(item_band_idx + 1, len(bands)):
        band = bands[bi]
        sw   = sorted(band['words'], key=lambda w: w[0])
        ints = []
        for w in sw:
            try:
                n = int(w[4].strip())
                if n > 0:
                    ints.append(((w[0] + w[2]) / 2, str(n)))
            except ValueError:
                pass
        non_empty = [w for w in sw if w[4].strip()]
        # QTY band must be ≥75% integers. Pure-quantity rows are 100%.
        # Part-number bands are ~50% integers (many alphanumeric catalog nos.)
        # so the 75% threshold cleanly separates qty from part bands.
        if non_empty and len(ints) / len(non_empty) >= 0.75:
            qty_band_idx = bi
            qty_xv = ints
            print(f"  [ColumnExtract] HorizBOM: qty  band y≈{band['y']:.1f}, "
                  f"{len(ints)}/{len(non_empty)} integer values", flush=True)
            break

    if qty_band_idx is None:
        print("  [ColumnExtract] HorizBOM: no QTY band found", flush=True)
        return ""

    # ── 4. Find PART band ───────────────────────────────────────────────────
    part_band_idx = None
    part_xv = []   # [(x_center, part_string)]

    for bi in range(qty_band_idx + 1, len(bands)):
        band = bands[bi]
        sw   = sorted(band['words'], key=lambda w: w[0])
        parts = [((w[0] + w[2]) / 2, w[4].strip())
                 for w in sw if len(w[4].strip()) >= 3]
        if len(parts) >= 5:
            part_band_idx = bi
            part_xv = parts
            print(f"  [ColumnExtract] HorizBOM: part band y≈{band['y']:.1f}, "
                  f"{len(parts)} values", flush=True)
            break

    if part_band_idx is None:
        print("  [ColumnExtract] HorizBOM: no PART band found", flush=True)
        return ""

    # ── 5. Collect MFG from remaining bands (may be multi-line) ────────────
    mfg_xv_all = []   # [(x_center, word)]
    for bi in range(part_band_idx + 1, len(bands)):
        band = bands[bi]
        for w in band['words']:
            txt = w[4].strip()
            if txt and len(txt) >= 2:
                mfg_xv_all.append(((w[0] + w[2]) / 2, txt))

    # ── 6. Build per-item MFG (nearest-x with multi-word accumulation) ──────
    item_mfg = {}
    for cx, txt in mfg_xv_all:
        best_item, best_dist = None, float('inf')
        for item_x, item_n in item_xn:
            d = abs(cx - item_x)
            if d < best_dist:
                best_dist, best_item = d, item_n
        if best_item is not None and best_dist <= col_half:
            item_mfg[best_item] = (item_mfg.get(best_item, "") + " " + txt).strip()

    # ── 7. Reconstruct vertical BOM table ───────────────────────────────────
    all_item_nums = sorted(n for _, n in item_xn)
    rows = ["ITEM | QTY | CATALOG NO. | MFG"]
    for item_n in all_item_nums:
        item_x = next(x for x, n in item_xn if n == item_n)
        qty    = nearest_by_x(qty_xv,  item_x)
        part   = nearest_by_x(part_xv, item_x)
        mfg    = item_mfg.get(item_n, "")
        row    = f"{item_n} | {qty} | {part} | {mfg}"
        rows.append(row)
        # Log a few rows for verification
        if item_n <= 3 or item_n in (26, 27, 36, 5):
            print(f"  [ColumnExtract] HorizBOM  item {item_n:2d}: "
                  f"qty={qty!r}  part={part!r}  mfg={mfg!r}", flush=True)

    result = "\n".join(rows)
    print(f"  [ColumnExtract] HorizBOM SUCCESS: {len(rows)-1} items, "
          f"{len(result)} chars", flush=True)
    return result


# ---------------------------------------------------------------------------
# Column-Aware BOM Extraction — THE FIX for qty/part-number digit merging
# ---------------------------------------------------------------------------
def _extract_bom_columns(pdf_bytes, page_numbers):
    """Extract BOM table using two strategies, best-first.

    Strategy 1 — find_tables() (PyMuPDF ≥ 1.23):
      Reads the physical grid lines drawn in the PDF to find table cells.
      Completely independent of column header x-positions.  If the BOM has
      visible cell borders (all CAD-generated BOMs do), this gives EXACT cell
      values with zero column-merging.  This is the slam-dunk path.

    Strategy 2 — word bounding-box fallback:
      Groups fitz words into visual rows and assigns to columns by midpoint
      between adjacent header x-positions.  Used when Strategy 1 finds no
      tables (borderless tables, old fitz version, etc.).

    Both strategies produce a pipe-separated table:
      ITEM | QTY | PART NUMBER | MANUFACTURER | DESCRIPTION
      26   | 14  | 3002619     | PHOENIX CONTACT | TERMINAL BLOCK...
    so QTY and PART NUMBER are in separate cells and can never merge.

    Args:
        pdf_bytes: Raw PDF bytes (not base64)
        page_numbers: List of 1-based page numbers

    Returns:
        Pipe-separated table string on success, "" on failure/not found.
    """
    _BOM_HEADER_KEYWORDS = {
        "item", "qty", "quantity", "part", "catalog", "no.", "no",
        "mfg", "manufacturer", "description", "desc", "ref", "tag",
        "number", "cat", "unit", "line",
    }

    # Strategy 2 constants (PDF points; 72pt = 1 inch)
    Y_TOLERANCE      = 8   # max y0 diff for words on the same visual line
    HEADER_MERGE_GAP = 20  # header words ≤20pt apart → same column label

    try:
        import fitz  # PyMuPDF

        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        total_pages = len(doc)
        all_rows = []

        for pg_num in page_numbers:
            idx = pg_num - 1
            if idx < 0 or idx >= total_pages:
                continue
            page = doc[idx]
            page_rows = []   # rows extracted from this page

            # ════════════════════════════════════════════════════════════════
            # STRATEGY 1: find_tables() — reads actual grid lines in the PDF
            # ════════════════════════════════════════════════════════════════
            try:
                tabs = page.find_tables()   # AttributeError if fitz < 1.23
                if tabs.tables:
                    # Pick the table with the most rows (= BOM, not title block)
                    best = max(tabs.tables, key=lambda t: len(t.rows))
                    print(f"  [ColumnExtract] Strat1 find_tables(): page {pg_num} → "
                          f"{len(tabs.tables)} table(s), using best "
                          f"({len(best.rows)} rows × {best.col_count if hasattr(best, 'col_count') else '?'} cols)",
                          flush=True)
                    # Title-block tables are tiny (≤ ~8 rows). If the largest
                    # table found is below this threshold the BOM wasn't detected
                    # by grid lines — fall through to Strategies 2 & 3.
                    if len(best.rows) < 9:
                        print(f"  [ColumnExtract] Strat1: only {len(best.rows)} rows "
                              f"— likely title block, not BOM; skipping to Strat2/3",
                              flush=True)
                        raise ValueError(f"table too small ({len(best.rows)} rows)")
                    n = 0
                    for row_data in best.extract():
                        # Normalise: collapse newlines inside cells, strip whitespace
                        cells = [str(c or "").replace("\n", " ").strip()
                                 for c in row_data]
                        row_text = " | ".join(cells)
                        if any(c for c in cells):
                            page_rows.append(row_text)
                            n += 1
                            if n <= 5:
                                print(f"  [ColumnExtract]   S1 row {n}: {row_text!r}",
                                      flush=True)
                    print(f"  [ColumnExtract] Strat1 SUCCESS: {n} rows extracted",
                          flush=True)
                else:
                    print(f"  [ColumnExtract] Strat1 find_tables(): page {pg_num} — "
                          f"no tables detected", flush=True)

            except AttributeError:
                # find_tables() not available (PyMuPDF < 1.23)
                print(f"  [ColumnExtract] Strat1 skipped: PyMuPDF < 1.23 "
                      f"(find_tables unavailable)", flush=True)
            except Exception as e1:
                print(f"  [ColumnExtract] Strat1 error: {e1}", flush=True)

            # ════════════════════════════════════════════════════════════════
            # STRATEGY 2: Horizontal / sideways BOM
            # Some CAD drawings store items left-to-right: all item numbers
            # are on one y-band, all quantities on the next, all parts on
            # the next.  _try_horizontal_bom() reconstructs vertical rows
            # by matching across bands by x-center proximity.
            # ════════════════════════════════════════════════════════════════
            raw_words = None   # fetch once, share between strategies 2 & 3
            if not page_rows:
                raw_words = page.get_text("words")
                if raw_words:
                    horiz = _try_horizontal_bom(raw_words, pg_num)
                    if horiz:
                        page_rows = horiz.splitlines()

            # ════════════════════════════════════════════════════════════════
            # STRATEGY 3: vertical word-bbox with header detection
            # (only runs if Strategies 1 & 2 got nothing)
            # ════════════════════════════════════════════════════════════════
            if not page_rows:
                print(f"  [ColumnExtract] Strat3 word-bbox header: page {pg_num}",
                      flush=True)
                if raw_words is None:
                    raw_words = page.get_text("words")
                if not raw_words:
                    print(f"  [ColumnExtract] Strat3: no words found on page {pg_num}",
                          flush=True)
                else:
                    raw_words = sorted(raw_words, key=lambda w: (w[1], w[0]))
                    print(f"  [ColumnExtract] Strat3: {len(raw_words)} words on page {pg_num}",
                          flush=True)

                    # Group into visual lines
                    lines, cur_line, line_y = [], [raw_words[0]], raw_words[0][1]
                    for w in raw_words[1:]:
                        if abs(w[1] - line_y) <= Y_TOLERANCE:
                            cur_line.append(w)
                        else:
                            lines.append(sorted(cur_line, key=lambda x: x[0]))
                            cur_line, line_y = [w], w[1]
                    if cur_line:
                        lines.append(sorted(cur_line, key=lambda x: x[0]))

                    # Log first 8 lines (text + x positions) for diagnosis
                    for di, dl in enumerate(lines[:8]):
                        words_info = "  ".join(
                            f"{w[4]!r}@{w[0]:.0f}" for w in dl)
                        print(f"  [ColumnExtract]   S3 line[{di:02d}] "
                              f"y≈{dl[0][1]:.1f}: {words_info}", flush=True)

                    col_headers = None
                    col_boundaries = None

                    for line_idx, line in enumerate(lines):
                        clean = [w for w in line if w[4].strip()]
                        if not clean:
                            continue
                        lower = [w[4].strip().lower() for w in clean]

                        # Header detection: ≥2 keywords on the same line
                        if col_headers is None:
                            matches = sum(1 for t in lower
                                          if t in _BOM_HEADER_KEYWORDS)
                            if matches >= 2:
                                # Cluster adjacent words into one column label
                                clusters, clust = [], [clean[0]]
                                for ci in range(1, len(clean)):
                                    gap = clean[ci][0] - clean[ci-1][2]
                                    if gap <= HEADER_MERGE_GAP:
                                        clust.append(clean[ci])
                                    else:
                                        clusters.append(clust)
                                        clust = [clean[ci]]
                                clusters.append(clust)

                                col_headers    = []
                                col_boundaries = []
                                for clust in clusters:
                                    lbl = " ".join(
                                        w[4].strip() for w in clust).upper()
                                    col_headers.append(lbl)
                                    col_boundaries.append(clust[0][0])

                                print(f"  [ColumnExtract] S3 header @ line {line_idx}:",
                                      flush=True)
                                for hi, (hn, hx) in enumerate(
                                        zip(col_headers, col_boundaries)):
                                    print(f"  [ColumnExtract]   S3col[{hi}] "
                                          f"x={hx:.1f}  {hn!r}", flush=True)
                                page_rows.append(" | ".join(col_headers))
                                continue

                        if col_headers is None:
                            continue

                        # Assign words to columns by midpoint boundary
                        n_cols = len(col_boundaries)
                        cells  = [""] * n_cols
                        for w in clean:
                            wx0   = w[0]
                            wtext = w[4].strip()
                            assigned = n_cols - 1
                            for ci in range(n_cols - 1):
                                mid = (col_boundaries[ci] +
                                       col_boundaries[ci + 1]) / 2
                                if wx0 < mid:
                                    assigned = ci
                                    break
                            cells[assigned] = (
                                cells[assigned] + " " + wtext).strip()

                        # Description continuation
                        ne = [i for i, c in enumerate(cells) if c.strip()]
                        if ne and ne == [n_cols - 1] and page_rows:
                            page_rows[-1] = (page_rows[-1].rstrip() + " "
                                             + cells[-1].strip())
                            continue

                        if any(c.strip() for c in cells):
                            row_text = " | ".join(cells)
                            page_rows.append(row_text)
                            if len(page_rows) <= 6:
                                print(f"  [ColumnExtract]   S3 row "
                                      f"{len(page_rows)}: {row_text!r}",
                                      flush=True)

                    if col_headers is None:
                        print(f"  [ColumnExtract] S2: no header found on "
                              f"page {pg_num}", flush=True)

            all_rows.extend(page_rows)

        doc.close()

        if not all_rows:
            print("  [ColumnExtract] Both strategies found nothing — "
                  "falling back to pypdf", flush=True)
            return ""

        result = "\n".join(all_rows)
        print(f"  [ColumnExtract] DONE: {len(all_rows)} rows "
              f"({len(result)} chars)", flush=True)
        return result

    except ImportError:
        print("  [ColumnExtract] fitz (PyMuPDF) not available", flush=True)
        return ""
    except Exception as exc:
        import traceback as _tb
        print(f"  [ColumnExtract] ERROR: {exc}", flush=True)
        print(_tb.format_exc(), flush=True)
        return ""


# ---------------------------------------------------------------------------
# High-DPI BOM crop rendering — v2.8 FIX for character accuracy
# ---------------------------------------------------------------------------
# Claude's vision API downscales images larger than ~1568px on the long side.
# A 3200x6600 image becomes ~764x1568 internally = ~11px/char = unreadable.
# Solution: render at 600 DPI, then TILE into sections of ~1500px height
# so Claude processes each tile at higher effective resolution.
# NO hard binary threshold — preserve anti-aliasing / gray levels that
# help Claude distinguish similar SHX characters (3/5, C/O, D/O).

def _enhance_for_vision(img):
    """Enhance thin SHX vector-font strokes for Claude's vision.

    Uses autocontrast + sharpen to improve readability WITHOUT destroying
    anti-aliasing. No binary threshold — gray levels help Claude distinguish
    similar characters (3 vs 5, D vs O) in SHX fonts.
    """
    from PIL import ImageFilter, ImageOps
    img = img.convert("L")
    img = ImageOps.autocontrast(img, cutoff=2)
    img = img.filter(ImageFilter.SHARPEN)
    img = img.filter(ImageFilter.SHARPEN)
    return img.convert("RGB")


def _render_bom_crops_hires_auto(pdf_b64, full_page_images, cropped_images,
                                  render_dpi=600):
    """Re-render BOM crops at high DPI, enhance strokes, and tile for Claude.

    1. Deduce crop coordinates by comparing full-page vs OCR-cropped sizes.
    2. Render the BOM area from the PDF at render_dpi (600).
    3. Apply stroke thickening for thin SHX fonts.
    4. Tile vertically into sections ≤ CLAUDE_MAX_DIM pixels tall so Claude
       processes each at high effective resolution (~40-50px per character).

    Returns:
        list of base64 PNG tiles (may be more than len(pages) due to tiling),
        or empty list on failure.
    """
    try:
        import fitz
        from PIL import Image

        pdf_bytes = base64.b64decode(pdf_b64)
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        hires = []

        for pg_idx in range(min(len(doc), len(full_page_images), len(cropped_images))):
            page = doc[pg_idx]

            # Get dimensions of the full-page render
            fp_bytes = base64.b64decode(full_page_images[pg_idx])
            fp_img = Image.open(io.BytesIO(fp_bytes))
            fp_w, fp_h = fp_img.size

            # Get dimensions of the OCR crop
            cr_bytes = base64.b64decode(cropped_images[pg_idx])
            cr_img = Image.open(io.BytesIO(cr_bytes))
            cr_w, cr_h = cr_img.size
            del fp_img, cr_img

            UPSCALE_W = 6000
            upscale_h = int(fp_h * (UPSCALE_W / fp_w))

            # BOM is right-aligned on panel drawings
            frac_x0 = max(0, (UPSCALE_W - cr_w - 60)) / UPSCALE_W
            frac_y0 = 0
            frac_x1 = 1.0
            frac_y1 = min(1.0, (cr_h + 120) / upscale_h)

            pw = page.rect.width
            ph = page.rect.height
            clip = fitz.Rect(
                frac_x0 * pw, frac_y0 * ph,
                frac_x1 * pw, frac_y1 * ph,
            )

            zoom = render_dpi / 72
            mat = fitz.Matrix(zoom, zoom)
            pix = page.get_pixmap(matrix=mat, clip=clip, alpha=False)

            # Convert to PIL for enhancement and tiling
            pil_img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
            pix_w, pix_h = pil_img.size
            del pix

            # Thicken thin SHX strokes for Claude's vision
            pil_img = _enhance_for_vision(pil_img)

            # Tile vertically so Claude sees each section at high resolution.
            # At 600 DPI, a ~3200x6600 crop → 5 tiles of ~3200x1500 each.
            # Claude scales each tile to ~1568x732 — much better than one
            # huge image that gets scaled to ~764x1568 (~11px/char).
            # Use overlap of ~80px so table rows at tile boundaries aren't split.
            tile_h = 1500
            overlap = 80
            tiles = []
            y = 0
            while y < pix_h:
                y_end = min(y + tile_h, pix_h)
                tile = pil_img.crop((0, y, pix_w, y_end))
                buf = io.BytesIO()
                tile.save(buf, format="PNG", optimize=True)
                tiles.append(base64.b64encode(buf.getvalue()).decode("ascii"))
                tile_kb = len(buf.getvalue()) / 1024
                print(f"  [HiRes] Page {pg_idx+1} tile {len(tiles)}: "
                      f"{pix_w}x{y_end-y} @ {render_dpi}DPI ({tile_kb:.0f}KB) "
                      f"[y={y}-{y_end}]", flush=True)
                del tile, buf
                if y_end >= pix_h:
                    break
                y = y_end - overlap  # overlap so rows aren't cut

            hires.extend(tiles)
            del pil_img
            import gc; gc.collect()

            print(f"  [HiRes] Page {pg_idx+1}: {len(tiles)} tile(s) from "
                  f"{pix_w}x{pix_h} crop "
                  f"[clip: x={frac_x0:.2f}-{frac_x1:.2f} y={frac_y0:.2f}-{frac_y1:.2f}]",
                  flush=True)

        doc.close()
        return hires

    except Exception as exc:
        print(f"  [HiRes] ERROR: {exc}", flush=True)
        return []




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
        MAX_LONG_SIDE_PX = 4500  # cap image long side — 4500px gives ~264 DPI on D-size
        # (17x11" at 264 DPI = 4488x2904px).  Empirically: 3000px (176 DPI) makes
        # small BOM text (~17px/char) unreadable for Claude; 4500px gives ~27px/char
        # which is reliably readable.  5500px was unnecessarily large — used only
        # for upload overhead, not accuracy (Claude tiles at ~512px anyway).

        for page_num in range(len(doc)):
            page = doc[page_num]
            # Adaptive DPI: cap so the long side never exceeds MAX_LONG_SIDE_PX pixels.
            # A D-size drawing at 400 DPI = ~13600px → ~200 MB image → Railway OOM.
            # At 5500px max the long side is still fully readable for OCR and Claude.
            long_side_pts = max(page.rect.width, page.rect.height)
            long_side_in  = long_side_pts / 72.0 if long_side_pts > 0 else 1.0
            max_dpi       = int(MAX_LONG_SIDE_PX / long_side_in)
            effective_dpi = min(dpi, max_dpi)
            if effective_dpi < dpi:
                print(f"  [Render] Page {page_num+1}: large page "
                      f"({page.rect.width/72:.1f}x{page.rect.height/72:.1f}in) — "
                      f"capping DPI {dpi}→{effective_dpi}", flush=True)
            zoom = effective_dpi / 72
            mat  = fitz.Matrix(zoom, zoom)
            pix  = page.get_pixmap(matrix=mat, alpha=False)
            png_bytes = pix.tobytes("png")
            img_b64 = base64.b64encode(png_bytes).decode("ascii")
            images_b64.append(img_b64)

            print(f"  [Render] Page {page_num+1}: {pix.width}x{pix.height} "
                  f"@ {effective_dpi}DPI, {len(png_bytes)/1024:.0f}KB PNG", flush=True)

        doc.close()

        total_kb = sum(len(img) * 3 / 4 / 1024 for img in images_b64)
        print(f"  [Render] Total: {len(images_b64)} page(s), "
              f"{total_kb:.0f}KB image data", flush=True)
        return images_b64

    except ImportError as exc:
        print(f"  [Render] PyMuPDF (fitz) import failed: {exc}",
              flush=True)
        return []
    except Exception as exc:
        print(f"  [Render] ERROR rendering PDF to image: {exc}", flush=True)
        return []


# ---------------------------------------------------------------------------
# Column-aware BOM table reconstruction from OCR word positions
# ---------------------------------------------------------------------------
_OCR_KW_TO_FIELD = {
    "no":           "item_num",
    "no.":          "item_num",
    "item":         "item_num",
    "item#":        "item_num",
    "description":  "description",
    "desc":         "description",
    "manufacturer": "manufacturer",
    "mfg":          "manufacturer",
    "vendor":       "manufacturer",
    "part":         "part_number",
    "catalog":      "part_number",
    "cat":          "part_number",
    "cat.":         "part_number",
    "p/n":          "part_number",
    "pn":           "part_number",
    "qty":          "qty",
    "quantity":     "qty",
}

def _ocr_build_column_table(words, img_width):
    """Given pytesseract word list (each word has left/top/right keys),
    locate the BOM table header row and reconstruct a pipe-separated
    column-aware table — the same format used for fitz column extraction.

    Uses keyword-based column anchoring: each BOM header keyword word
    (QTY, PART, DESCRIPTION, etc.) becomes its own column at its exact
    x-position, regardless of pixel gap.  This prevents gap-based merging
    of adjacent headers like PART NUMBER and QTY.

    Returns a non-empty string on success, "" if no BOM table detected.
    """
    Y_ROW_TOL = 22   # px: words within this vertical range = same row
    MIN_KW    = 3    # BOM header must contain ≥ this many keywords

    # ── 1. Group words into rows by y-position ──────────────────────────────
    rows = []   # each entry: [avg_y, [word, ...]]
    for w in sorted(words, key=lambda x: x["top"]):
        for row in rows:
            if abs(w["top"] - row[0]) <= Y_ROW_TOL:
                row[1].append(w)
                break
        else:
            rows.append([w["top"], [w]])
    rows = [(y, sorted(ws, key=lambda x: x["left"])) for y, ws in rows]

    # ── 2. Find the BOM header row ───────────────────────────────────────────
    header_idx   = None
    header_words = None
    for i, (y, rw) in enumerate(rows):
        tokens = [w["text"].lower().rstrip(".#:") for w in rw]
        kw_hits = sum(1 for t in tokens if t in _OCR_KW_TO_FIELD)
        if kw_hits >= MIN_KW:
            header_idx   = i
            header_words = rw
            break
    if header_idx is None:
        print("  [OCR-cols] no BOM header row found", flush=True)
        return "", None

    # ── 3. Keyword-anchored column detection ─────────────────────────────────
    # Each keyword word starts a new column at its own x-centre.
    # Non-keyword words (e.g. "NUMBER" in "PART NUMBER") append to the
    # current column label without shifting the boundary.
    #
    # Special case: "NO" or "NO." after a part_number column means it is the
    # trailing word of "CATALOG NO." — do NOT start a new item_num column.
    # cols entries: {"label": str, "x_left": int, "x_right": int}
    # x_left  = left edge of the keyword word (first word of the column)
    # x_right = right edge of the rightmost word appended to this column
    # Using the FULL SPAN of all header words in a column gives accurate
    # gap-based boundaries (whitespace between columns), which is more robust
    # than midpoint-of-keyword-centers (which shifts when multi-word headers
    # like "PART NUMBER" are anchored at the first word only).
    cols = []
    last_field = None
    for w in header_words:
        tok   = w["text"].lower().rstrip(".#:")
        raw   = w["text"].upper()

        if tok in _OCR_KW_TO_FIELD:
            field = _OCR_KW_TO_FIELD[tok]
            # "NO"/"NO." after part_number = "CATALOG NO." suffix, not new column
            if tok in ("no", "no.") and last_field == "part_number":
                if cols:
                    cols[-1]["label"]   += " " + raw
                    cols[-1]["x_right"]  = max(cols[-1]["x_right"], w["right"])
                # do NOT start a new column
            else:
                cols.append({"label": raw,
                             "x_left":  w["left"],
                             "x_right": w["right"]})
                last_field = field
        else:
            # Non-keyword: append to current column label and extend its span
            if cols:
                cols[-1]["label"]   += " " + raw
                cols[-1]["x_right"]  = max(cols[-1]["x_right"], w["right"])

    if len(cols) < 3:
        # Fallback: gap-based clustering (120 px threshold)
        print(f"  [OCR-cols] only {len(cols)} keyword cols — "
              f"falling back to gap-based clustering", flush=True)
        MERGE_GAP = 120
        clusters = [[header_words[0]]]
        for w in header_words[1:]:
            gap = w["left"] - clusters[-1][-1]["right"]
            if gap <= MERGE_GAP:
                clusters[-1].append(w)
            else:
                clusters.append([w])
        col_labels = [" ".join(w["text"].upper() for w in cl) for cl in clusters]
        # Use actual span for boundaries
        col_spans  = [(cl[0]["left"], cl[-1]["right"]) for cl in clusters]
        tbl_x_min  = col_spans[0][0]  - 150
        tbl_x_max  = col_spans[-1][1] + 150
        if len(col_labels) < 3:
            print(f"  [OCR-cols] fallback also < 3 cols", flush=True)
            return "", None
        # Build gap-based boundaries
        boundaries = []
        for i in range(len(col_spans) - 1):
            gap_start = col_spans[i][1]
            gap_end   = col_spans[i + 1][0]
            if gap_end > gap_start:
                boundaries.append((gap_start + gap_end) // 2)
            else:
                # Overlapping headers — use centre midpoints
                c_i    = (col_spans[i][0]     + col_spans[i][1])     // 2
                c_next = (col_spans[i + 1][0] + col_spans[i + 1][1]) // 2
                boundaries.append((c_i + c_next) // 2)
        boundaries.append(img_width + 99999)
    else:
        col_labels = [c["label"]   for c in cols]
        tbl_x_min  = cols[0]["x_left"]  - 150
        tbl_x_max  = cols[-1]["x_right"] + 150
        # Gap-based boundaries: midpoint of whitespace between adjacent columns
        boundaries = []
        for i in range(len(cols) - 1):
            gap_start = cols[i]["x_right"]
            gap_end   = cols[i + 1]["x_left"]
            if gap_end > gap_start:
                boundaries.append((gap_start + gap_end) // 2)
            else:
                # Headers overlap — midpoint of their full spans
                c_i    = (cols[i]["x_left"]     + cols[i]["x_right"])     // 2
                c_next = (cols[i + 1]["x_left"] + cols[i + 1]["x_right"]) // 2
                boundaries.append((c_i + c_next) // 2)
        boundaries.append(img_width + 99999)

    def assign_col(word_cx):
        for ci, bnd in enumerate(boundaries):
            if word_cx <= bnd:
                return ci
        return len(col_labels) - 1

    print(f"  [OCR-cols] {len(col_labels)} cols @ row {header_idx}: "
          f"{col_labels}", flush=True)
    print(f"  [OCR-cols] boundaries (px): {boundaries[:-1]}  "
          f"tbl_x=[{tbl_x_min},{tbl_x_max}]", flush=True)

    # ── 4. Build the structured table ────────────────────────────────────────
    table_rows = [" | ".join(col_labels)]
    for _y, rw in rows[header_idx + 1:]:
        tw = [w for w in rw
              if w["left"] >= tbl_x_min and w["right"] <= tbl_x_max]
        if not tw:
            continue
        cells = [""] * len(col_labels)
        for w in tw:
            cx = (w["left"] + w["right"]) // 2
            ci = assign_col(cx)
            cells[ci] = (cells[ci] + " " + w["text"]).strip()
        if any(c for c in cells):
            table_rows.append(" | ".join(cells))

    # ── 4b. Blank QTY column from the structured table ────────────────────────
    # OCR single-digit recognition is imperfect: a "3" can be read as "1", and
    # vertical table border lines are sometimes read as "1".  If the column
    # table has a wrong QTY value, Stage 2 may anchor to it despite the
    # "read QTY from image" instruction.  Blanking the QTY cells entirely
    # forces Stage 2 to derive every QTY purely from the image — which is
    # correct because the cropped image now has ~3-4× more pixels per cell.
    qty_col_idx = next(
        (i for i, lbl in enumerate(col_labels)
         if lbl.upper().strip() in ("QTY", "QUANTITY")),
        None,
    )
    if qty_col_idx is not None:
        blanked = []
        for row_str in table_rows[1:]:
            cells = row_str.split(" | ")
            while len(cells) <= qty_col_idx:
                cells.append("")
            cells[qty_col_idx] = ""   # wipe OCR-sourced QTY value
            blanked.append(" | ".join(cells))
        table_rows = [table_rows[0]] + blanked
        print(f"  [OCR-cols] QTY col ({qty_col_idx}) blanked — "
              f"Stage 2 reads qty from image only", flush=True)

    result = "\n".join(table_rows)
    print(f"  [OCR-cols] built {len(table_rows)-1} data rows, "
          f"{len(result)} chars", flush=True)
    for ri, line in enumerate(table_rows[1:6]):
        print(f"  [OCR-cols] row[{ri+1}]: {line!r}", flush=True)

    # ── 5. Compute crop box so caller can send a tight BOM-only image to Stage 2 ──
    # The full drawing page may be much wider than the BOM table (panel layout on
    # the left etc.).  Sending a cropped image gives Claude 3-4× more pixels per
    # cell and eliminates confusion from non-BOM numbers on the same page.
    header_y_top = min(w["top"] for w in header_words)
    last_y_bottom = header_y_top
    for _y, rw in rows[header_idx + 1:]:
        tw = [w for w in rw if w["left"] >= tbl_x_min and w["right"] <= tbl_x_max]
        if tw:
            bottom = max(w["top"] + w.get("height", 25) for w in tw)
            last_y_bottom = max(last_y_bottom, bottom)
    crop_box = (
        max(0, tbl_x_min - 60),        # x0: small left margin
        max(0, header_y_top - 100),     # y0: above header row
        tbl_x_max + 60,                 # x1: small right margin
        last_y_bottom + 120,            # y1: below last data row
    )
    print(f"  [OCR-cols] crop_box: x={crop_box[0]}-{crop_box[2]}  "
          f"y={crop_box[1]}-{crop_box[3]}", flush=True)
    return result, crop_box


# ---------------------------------------------------------------------------
# OCR — extract exact text from rendered BOM images using pytesseract
# ---------------------------------------------------------------------------
def _ocr_images(images_b64):
    """Run pytesseract OCR on rendered BOM page images.

    Preprocesses the image for thin-stroke AutoCAD SHX fonts (grayscale,
    auto-contrast, stroke dilation) then uses word-position bounding boxes
    to reconstruct table columns accurately instead of reading across rows.
    The resulting text is the PRIMARY source for Stage 2 so Claude structures
    already-correct characters into JSON rather than doing OCR itself.
    """
    try:
        import gc
        import pytesseract
        from PIL import Image, ImageOps, ImageFilter

        all_text = []
        cropped_images_b64 = []   # tight BOM-table crops, one per page (when detected)
        for i, img_b64 in enumerate(images_b64):
            img_bytes = base64.b64decode(img_b64)
            img = Image.open(io.BytesIO(img_bytes))

            # --- Preprocess for thin single-stroke CAD fonts ---
            # 0. Upscale if image is smaller than the OCR minimum.
            #    Tesseract accuracy degrades sharply below ~300 DPI equivalent;
            #    character pairs like 0/C, 0/D, H/M, 5/6, S/8 become ambiguous.
            #    Target 6000px wide → ~353 DPI for 17×11" drawings — enough to
            #    keep all character pairs visually distinct.
            #    NOTE: 8000px was tried but causes OOM on Railway with multi-page
            #    BOMs (each page = ~124MB raw + 5 preprocessing copies = ~620MB).
            MIN_OCR_WIDTH = 6000
            if img.width < MIN_OCR_WIDTH:
                scale = MIN_OCR_WIDTH / img.width
                new_w = int(img.width  * scale)
                new_h = int(img.height * scale)
                img = img.resize((new_w, new_h), Image.LANCZOS)
                print(f"  [OCR] Page {i+1}: upscaled {img.width}×{img.height}px "
                      f"for better character accuracy", flush=True)
            # Save dimensions before preprocessing (img is referenced later
            # for crop_box and column width)
            img_width = img.width
            img_height = img.height
            # 1. Grayscale
            img_gray = img.convert("L")
            # 2. Auto-contrast: normalise brightness range
            img_enhanced = ImageOps.autocontrast(img_gray, cutoff=2)
            del img_gray
            # 3. Dilate strokes: MaxFilter thickens 1-2px lines so
            #    Tesseract can distinguish characters reliably
            img_dilated = img_enhanced.filter(ImageFilter.MaxFilter(3))
            del img_enhanced
            # 4. Sharpen to restore edge definition after dilation
            img_sharp = img_dilated.filter(ImageFilter.SHARPEN)
            del img_dilated

            # --- Word-position OCR (PSM 11 = sparse, no layout assumptions) ---
            data = pytesseract.image_to_data(
                img_sharp,
                config="--psm 11 --oem 3",
                output_type=pytesseract.Output.DICT,
            )

            # Build word list: keep anything tesseract detected (conf >= 0),
            # only discard conf=-1 (whitespace/empty blocks tesseract skipped).
            # Threshold was conf>10 but that was dropping word-final characters
            # like the "IT" in "PANDUIT" when SHX thin strokes scored low.
            words = []
            for j in range(len(data["text"])):
                conf = int(data["conf"][j])
                txt = data["text"][j].strip()
                if conf >= 0 and txt:
                    words.append({
                        "text":   txt,
                        "left":   int(data["left"][j]),
                        "top":    int(data["top"][j]),
                        "right":  int(data["left"][j]) + int(data["width"][j]),
                        "height": int(data["height"][j]),
                    })

            if not words:
                print(f"  [OCR] Page {i + 1}: no words detected", flush=True)
                all_text.append("")
                continue

            # Sort by vertical position then horizontal
            words.sort(key=lambda w: (w["top"], w["left"]))

            # ── Try column-aware BOM table extraction first ──────────────────
            col_text, crop_box = _ocr_build_column_table(words, img_width)
            if col_text.strip():
                all_text.append(col_text)
                # Crop the rendered image to just the BOM table area.
                # The full page may be 3-4× wider than the BOM (panel layout on
                # the left), so cropping gives Claude ~3-4× more pixels per cell,
                # making single-digit QTY values reliably readable.
                # Use img (original) for cropping — it has the right pixel coords.
                if crop_box is not None:
                    x0, y0, x1, y1 = crop_box
                    x0 = max(0, x0);  y0 = max(0, y0)
                    x1 = min(img_width, x1);  y1 = min(img_height, y1)
                    cropped = img.crop((x0, y0, x1, y1))
                    buf = io.BytesIO()
                    cropped.save(buf, format="PNG")
                    cropped_b64 = base64.b64encode(buf.getvalue()).decode()
                    cropped_images_b64.append(cropped_b64)
                    del cropped
                    print(f"  [OCR] Page {i+1}: cropped BOM table "
                          f"{x1-x0}×{y1-y0}px  (full page was "
                          f"{img_width}×{img_height}px)", flush=True)
                print(f"  [OCR] Page {i + 1}: {len(words)} words → "
                      f"column-aware table ({len(col_text)} chars)", flush=True)
                # Free memory before next page
                del img, img_sharp, data, words
                gc.collect()
                continue  # skip the flat fallback below

            # ── Fallback: flat line reconstruction with gap-based | markers ──
            print(f"  [OCR] Page {i + 1}: column extraction failed — "
                  f"falling back to flat OCR", flush=True)

            # Group words into lines (within 25px vertically)
            lines, current_line, line_y = [], [words[0]], words[0]["top"]
            for w in words[1:]:
                if abs(w["top"] - line_y) <= 25:
                    current_line.append(w)
                else:
                    lines.append(sorted(current_line, key=lambda x: x["left"]))
                    current_line = [w]
                    line_y = w["top"]
            if current_line:
                lines.append(sorted(current_line, key=lambda x: x["left"]))

            # Reconstruct text preserving column gaps with " | " markers
            text_lines = []
            for line in lines:
                parts, prev_right = [], 0
                for w in line:
                    gap = w["left"] - prev_right
                    if parts and gap > 80:
                        parts.append(" | ")
                    elif parts and gap > 15:
                        parts.append(" ")
                    parts.append(w["text"])
                    prev_right = w["right"]
                text_lines.append("".join(parts))

            text = "\n".join(text_lines)
            all_text.append(text)
            print(f"  [OCR] Page {i + 1}: {len(words)} words, "
                  f"{len(lines)} lines, {len(text)} chars", flush=True)
            print(f"  [OCR] First 400: {text[:400]!r}", flush=True)

            # Free memory between pages — multi-page BOMs can OOM on Railway
            del img, img_sharp, data, words
            gc.collect()

        combined = "\n".join(all_text)
        print(f"  [OCR] Total: {len(combined)} chars across "
              f"{len(images_b64)} page(s)", flush=True)
        if cropped_images_b64:
            print(f"  [OCR] Cropped BOM images: {len(cropped_images_b64)} "
                  f"page(s) — Stage 2 will use these for higher-resolution "
                  f"cell reading", flush=True)
        return combined, cropped_images_b64

    except ImportError as exc:
        print(f"  [OCR] pytesseract not available: {exc}", flush=True)
        return "", []
    except Exception as exc:
        print(f"  [OCR] ERROR: {exc}", flush=True)
        return "", []


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
        # Anthropic requires api max_tokens = thinking_budget + output_tokens.
        # claude-sonnet-4 hard cap is 64 000 total — enforce it here so callers
        # don't need to know the limit.
        API_HARD_CAP = 64000
        output_tokens = max_tokens  # caller's max_tokens = desired output portion
        if thinking_budget + output_tokens > API_HARD_CAP:
            # Shrink thinking first — output JSON is mandatory, thinking is best-effort
            thinking_budget = max(1024, API_HARD_CAP - output_tokens)
            if output_tokens > API_HARD_CAP - 1024:
                # Even output alone nearly exceeds cap (very large drawing)
                output_tokens = API_HARD_CAP - 1024
                thinking_budget = 1024
            print(f"  [API] Token budget capped to {API_HARD_CAP}: "
                  f"thinking={thinking_budget}, output={output_tokens}", flush=True)
        api_kwargs["max_tokens"] = thinking_budget + output_tokens
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
                print(f"  [{stage_label}] Stripped {first_brace} chars of preamble: "
                      f"{raw[:min(400, first_brace)]!r}",
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
def _stage1_detect_structure(claude_client, pdf_b64, thinking_budget=10000):
    prompt = (
        "You are reading an industrial electrical panel drawing PDF.\n"
        "Your ONLY job is to find the BOM (Bill of Materials) table and report its structure.\n\n"
        "DO NOT extract any row data yet. Just report the table structure.\n\n"
        "Look through EVERY page. Find any table that lists parts/components with quantities.\n\n"
        "Return this JSON:\n"
        "{\n"
        '  "bom_tables_found": 1,\n'
        '  "drawing_types_found": ["BOM"],\n'
        '  "pages_with_bom": [3],\n'
        '  "column_headers_left_to_right": ["ITEM", "DESCRIPTION", "MFG", "CATALOG NO.", "QTY"],\n'
        '  "column_mapping": {\n'
        '    "item_num": "ITEM",\n'
        '    "qty": "QTY",\n'
        '    "part_number": "CATALOG NO.",\n'
        '    "manufacturer": "MFG",\n'
        '    "description": "DESCRIPTION"\n'
        "  },\n"
        '  "total_bom_rows": 47,\n'
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
        "Count CAREFULLY -- go row by row and count each one.\n"
        "If the BOM spans MULTIPLE pages, add up ALL rows from ALL BOM pages.\n"
        "CRITICAL: total_bom_rows MUST be >= 1 if bom_tables_found >= 1. "
        "Never return 0 if you found a BOM table — count carefully and report the real number.\n\n"
        "CRITICAL: Report the EXACT page number(s) where the BOM table appears in pages_with_bom.\n"
        "This drawing may have 30+ pages of schematics, wiring diagrams, etc.\n\n"
        "WHERE TO FIND THE BOM:\n"
        '- Look for tables titled "BILL OF MATERIALS", "BOM", "B.O.M.", "PARTS LIST", or "MATERIAL LIST"\n'
        "- The BOM may appear on a DEDICATED page, OR it may be embedded within a schematic page\n"
        '- Pages titled "Schematic and B.O.M." or similar CONTAIN a BOM — include those pages!\n'
        "- The BOM could be on page 1, the last page, or ANY page in between — check them ALL\n"
        "- If the BOM spans multiple pages (e.g., B.O.M. I, B.O.M. II, B.O.M. III), list ALL those pages\n\n"
        "WHAT IS NOT A BOM:\n"
        "- Wire schedules (list wire numbers and terminations — no part numbers)\n"
        "- Terminal schedules (list terminal block assignments)\n"
        "- Nameplate schedules (list nameplates/labels)\n"
        "- Cable schedules\n"
        "- Pages with ONLY schematics and NO parts table are not BOM pages\n"
        "- But a page with BOTH a schematic AND a parts/BOM table IS a BOM page — include it!"
    )

    result = _call_claude(claude_client, pdf_b64, prompt,
                          thinking_budget=thinking_budget, max_tokens=4000,
                          stage_label="Stage1")
    print(f"SCAN Stage 1: Found {result.get('bom_tables_found', 0)} BOM table(s), "
          f"{result.get('total_bom_rows', '?')} rows, "
          f"headers={result.get('column_headers_left_to_right', [])}", flush=True)
    # Log column_mapping so we can verify correct field↔label assignments
    col_map = result.get("column_mapping", {})
    print(f"  [Stage1] column_mapping: "
          f"item_num→'{col_map.get('item_num','')}' "
          f"qty→'{col_map.get('qty','')}' "
          f"part_number→'{col_map.get('part_number','')}' "
          f"manufacturer→'{col_map.get('manufacturer','')}' "
          f"description→'{col_map.get('description','')}'", flush=True)
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
                "The BOM table is provided as HIGH-RESOLUTION image tile(s).\n"
                "Each tall BOM page has been split into multiple overlapping vertical tiles\n"
                "so you can read each character clearly. The tiles are in order from top to\n"
                "bottom. Some rows may appear in two adjacent tiles (overlap zone) — only\n"
                "extract each row ONCE.\n"
                "The BILL OF MATERIALS table may occupy only PART of the image — the rest\n"
                "may show panel layouts, schematics, or other diagrams.\n"
                "FIND the BILL OF MATERIALS table (look for a grid with headers like\n"
                "NO./ITEM, DESCRIPTION, MANUFACTURER, PART NUMBER, QTY).\n"
                "Read EVERY row of that table. IGNORE everything outside the table.\n\n"
                "=== CRITICAL: READ THE IMAGE — DO NOT INVENT, DO NOT REFUSE ===\n"
                "RULE 1 — Never skip rows: Return a JSON entry for EVERY numbered row in\n"
                "the BOM table. Returning 0 items when a table is visible is always wrong.\n"
                "RULE 2 — Never use [?] for a whole field: [?] is only for ONE unreadable\n"
                "character inside a value (e.g. 'HWI[?]-2CE1T24'). If you can read most of\n"
                "a part number, write what you can read. An imperfect read is better than [?].\n"
                "RULE 3 — Never substitute: do not replace what you see with a similar part\n"
                "number from your training data. Read character-by-character what is shown.\n"
                "RULE 4 — Partial data is fine: a row with a blank or partial part number is\n"
                "far better than a missing row. Give your best visual read for every cell.\n\n"
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

    raw_text = structure.get("_bom_raw_text", "")
    text_source = structure.get("_bom_text_source", "pypdf")
    if raw_text.strip():
        if text_source == "columns":
            # Best case: fitz word bounding boxes → pipe-separated table.
            # Each '|' separator is a true column boundary — merging is impossible.
            # QTY and PART NUMBER are always in separate cells.
            text_section = (
                "=== STRUCTURED BOM TABLE (COLUMN-PARSED) ===\n"
                "The following table was built by detecting column positions from the drawing.\n"
                "Each cell between '|' separators is a single column value.\n"
                "PART NUMBER and MANUFACTURER cells are reliable — use them directly.\n"
                "QTY cells may be misread by OCR — always verify QTY against the IMAGE.\n\n"
                f"{raw_text}\n\n"
                "=== END STRUCTURED TABLE ===\n\n"
            )
        elif text_source == "ocr":
            # SHX / vector-font drawing — text came from pytesseract.
            # v2.8: Combined approach — OCR provides STRUCTURE (format, length,
            # hyphens, column alignment), high-res image provides CHARACTER
            # verification. OCR is reliable for multi-char sequences but can
            # confuse similar SHX glyphs (D↔O, 3↔5, 0↔O). The 600 DPI image
            # has ~50px per character which helps distinguish these.
            text_section = (
                "=== OCR TEXT FROM BOM IMAGE (PYTESSERACT) ===\n"
                "The following text was extracted by pytesseract OCR from the rendered BOM image.\n"
                "It provides character-by-character data for each BOM row.\n\n"
                "HOW TO USE OCR + IMAGE TOGETHER:\n"
                "  This is a thin-stroke SHX vector font drawing. Both OCR and visual reading\n"
                "  can make errors on similar-looking characters. Use BOTH sources together:\n\n"
                "  PART NUMBER:\n"
                "    1. Start with the OCR text — it gives you the correct NUMBER of characters,\n"
                "       hyphens, slashes, and overall structure of each part number.\n"
                "    2. Do NOT transpose, rearrange, or drop characters from the OCR reading.\n"
                "       If OCR says IR18219K, do NOT change it to IR1821K9.\n"
                "    3. Verify EACH character against the HIGH-RES IMAGE. SHX fonts confuse:\n"
                "       D↔O (both oval), 3↔5 (both have curves), S↔5 (nearly identical in SHX!),\n"
                "       0↔O, 6↔8, B↔8, 1↔I, C↔O\n"
                "       CONTEXT HELPS: if a character could be S or 5, consider whether it is\n"
                "       part of a letter sequence (→ S) or a digit sequence (→ 5).\n"
                "       Example: 'BOOS' = letters → S not 5.  '1800' = digits → 8 not B.\n"
                "       When the image clearly shows a different character, USE THE IMAGE.\n"
                "       Example: OCR says 'OTC2AP' but image clearly shows 'DTC2AP' → use DTC2AP.\n"
                "    4. For trailing characters (end of part number), look especially carefully —\n"
                "       OCR often drops the last character if it's near a column border.\n"
                "       Example: OCR says 'BOOS-K' but image shows 'BOOS-K9' → use BOOS-K9.\n\n"
                "  DESCRIPTION / MANUFACTURER — Use OCR as guide, fill in truncations from image.\n\n"
                "  QTY — READ FROM THE IMAGE ONLY. IGNORE OCR QTY VALUES.\n"
                "    OCR misreads single-digit numbers and column boundaries.\n"
                "    Always read QTY by looking at the rightmost column in the image.\n\n"
                "  ITEM NUMBER (NO.) — Use OCR text, verify sequence against image.\n\n"
                f"{raw_text}\n\n"
                "=== END OCR TEXT ===\n\n"
            )
        else:
            # Normal PDF with real text layer — pypdf extracted exact characters.
            # This text is authoritative; image is for layout/structure only.
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

    # When using images, drop the column-order hint — Claude can read headers
    # directly from the image. Only pass field-name mappings as a reference.
    if using_images:
        col_structure_section = (
            f"FIELD MAPPING (map each column header you see to these fields):\n"
            f"{mapping_info}\n\n"
            "Read the HEADER ROW of the BOM table in the image to determine the actual\n"
            "column order. Do NOT assume column positions — read them from the image.\n\n"
        )
    else:
        col_structure_section = (
            f"COLUMN STRUCTURE (already identified):\n{col_info}\n\n"
            f"FIELD MAPPING:\n{mapping_info}\n\n"
        )

    prompt = (
        "You are transcribing the BOM (Bill of Materials) table from an electrical panel drawing.\n\n"
        f"{page_instruction}"
        f"{text_section}"
        f"{col_structure_section}"
        f"Has manufacturer column: {has_mfg}\n"
        f"Has description column: {has_desc}\n"
        f"Expected row count: {total_rows} numbered items — but also extract any "
        f"un-numbered sub-item or accessory rows (they appear without an item number "
        f"directly below their parent item). The total may exceed {total_rows}.\n\n"

        "YOUR TASK: Read EVERY row of the BOM table. Copy each cell value EXACTLY as printed.\n\n"

        "=== CRITICAL RULES ===\n\n"

        "RULE 1 — ONE ROW = ONE ITEM, READ QTY EXACTLY AS PRINTED:\n"
        "If the QTY column says 125, output qty:125 as ONE item.\n"
        "Do NOT create multiple items from one row. That is wrong.\n"
        "Quantities ARE frequently large numbers — 50, 100, 125, 200, 500+ are all NORMAL.\n"
        "Items like terminal blocks, cable ties, wire duct, DIN rail, markers are routinely 100+.\n"
        "A 3-digit quantity like 125 is NOT unusual — do NOT round down or truncate it to 13 or 12.\n"
        "If a description spans multiple lines in the table, ALL lines belong to ONE item with ONE qty.\n"
        "SELF-CHECK every qty: if the number seems unusually small for the part type, re-read the cell.\n\n"

        "RULE 2 — PART NUMBERS ARE SACRED:\n"
        "These part numbers will be used to ORDER real parts. A wrong character = wrong part delivered.\n"
        "Copy EVERY character EXACTLY: dashes, slashes, letters, numbers, spaces.\n"
    )

    # Add extra instructions depending on whether we have raw text
    if raw_text.strip():
        if text_source == "columns":
            prompt += (
                "  STRUCTURED TABLE — field-specific trust rules:\n\n"
                "  PART NUMBER — use the table cell DIRECTLY (most reliable source):\n"
                "    The PART NUMBER column cell is exact. Copy it character-for-character.\n"
                "    Do NOT re-read part numbers from the image — the table value is authoritative.\n\n"
                "  MANUFACTURER — use the table cell directly.\n\n"
                "  DESCRIPTION — use the table cell as a guide; correct obvious OCR garbling from the image.\n\n"
                "  QTY — READ FROM THE IMAGE, NOT THE TABLE:\n"
                "    OCR misreads single-digit numbers (3→8, 1→7, 6→b, 5→S) and column boundary\n"
                "    math can place a QTY value in the wrong cell entirely.\n"
                "    The table QTY cell is UNRELIABLE. Always read QTY from the image.\n\n"
                "  === QTY — IMAGE READING RULES ===\n"
                "  1. In the image, find the HEADER ROW. The QTY column is the RIGHTMOST column.\n"
                "  2. For EACH data row: read the number in that row's rightmost (QTY) cell.\n"
                "  3. Read each row INDEPENDENTLY — never carry over qty from the previous row.\n"
                "  4. Both 'NO.' (item number) and 'QTY' are narrow integer columns — do not swap them:\n"
                "       LEFTMOST narrow column  = item_num  (counts up: 1, 2, 3 ...)\n"
                "       RIGHTMOST narrow column = QTY       (can be any integer: 1, 6, 14, 134 ...)\n"
                "  5. If a qty looks like an item number sequence (1, 2, 3, 4...) you have them swapped.\n\n"
                "  Use the image to confirm row count and catch rows the table may have missed.\n\n"
            )
        elif text_source == "ocr":
            prompt += (
                "  === PART NUMBERS — USE OCR STRUCTURE + IMAGE VERIFICATION ===\n"
                "  The OCR text provides the STRUCTURE of each part number (number of characters,\n"
                "  hyphens, slashes, format). Start with the OCR reading.\n"
                "  Then VERIFY each character against the high-res image, especially for SHX\n"
                "  font characters that OCR commonly confuses:\n"
                "    D↔O (oval shapes), 3↔5, S↔5 (nearly identical!), 0↔O, C↔O, 6↔8, B↔8, 1↔I\n"
                "  If the image CLEARLY shows a different character than OCR, use the image.\n"
                "  Example: OCR='OTC2AP' but image shows 'DTC2AP' → use DTC2AP.\n"
                "  NEVER transpose or rearrange character ORDER from OCR (e.g. do NOT change\n"
                "  IR18219K to IR1821K9 — keep OCR's character sequence).\n"
                "  Check the LAST character carefully — OCR often drops trailing characters\n"
                "  near column borders. If the image shows an extra character, include it.\n\n"
                "  === MANUFACTURER — Fill in truncated names from the image ===\n"
                "  OCR often truncates manufacturer names (e.g. 'EN-BRADLEY' → 'ALLEN-BRADLEY',\n"
                "  'PHOENIX CONTAC' → 'PHOENIX CONTACT'). Use the image to complete these.\n\n"
                "  === QTY COLUMN — CRITICAL READING RULES ===\n"
                "  1. Look at the HEADER ROW in the image. Identify every column heading.\n"
                "     The QTY (or QUANTITY) column is typically the RIGHTMOST column.\n"
                "  2. For EACH row: find that row's cell in the QTY column and read the number.\n"
                "     Read it INDEPENDENTLY — do not carry over the previous row's quantity.\n"
                "  3. The OCR may merge 'PART NUMBER' and 'QTY' into one header — IGNORE that.\n"
                "     Trust the IMAGE column layout, not the OCR header text.\n"
                "  4. Common SHX font OCR mistakes in QTY: '1' read as 'l', '6' as 'b', '0' as 'O'.\n"
                "     When in doubt, zoom mentally into that specific cell and re-read.\n\n"
                "  === ITEM NUMBER (NO.) COLUMN — CRITICAL READING RULES ===\n"
                "  1. The NO. or ITEM column is typically the LEFTMOST column.\n"
                "  2. Numbered rows: read the item number exactly (1, 2, 3 ...).\n"
                "  3. Sub-item / accessory rows have a BLANK NO. cell — output item_num=0.\n"
                "     Do NOT carry forward the parent item's number into the sub-item row.\n"
                "  4. After a sub-item row, the NEXT numbered item resumes the sequence.\n"
                "     Stay synchronized with the actual row you are reading in the image.\n\n"
                "  === ANTI-SWAP CHECK (LEFTMOST vs RIGHTMOST NARROW COLUMN) ===\n"
                "  A BOM has TWO narrow integer columns that look similar:\n"
                "    - LEFTMOST narrow column  = Item Number  (1, 2, 3 ... sequential)\n"
                "    - RIGHTMOST narrow column = QTY          (can repeat: 1,1,1,6,6,1,...)\n"
                "  These are EASY TO SWAP. After reading every row, ask:\n"
                "    'Is my item_num value from the LEFT edge of the table?'  (YES = correct)\n"
                "    'Is my qty value from the RIGHT edge of the table?'      (YES = correct)\n"
                "  If you accidentally wrote the left-column number as qty and the right as item_num,\n"
                "  your output will look like: item_num=6, qty=21 for what should be item_num=21, qty=6.\n"
                "  Always verify: item_num counts sequentially row by row; qty can be any positive integer.\n\n"
            )
        else:
            prompt += (
                "  PART NUMBERS — use the RAW TEXT as your primary source:\n"
                "  The raw text contains the exact part number characters from the PDF.\n"
                "  Cross-reference every part number against the raw text and use the exact string.\n"
                "  Do NOT modify, correct, or 'improve' part numbers from the raw text.\n\n"
                "  QUANTITIES — READ FROM THE IMAGE ONLY. IGNORE ALL RAW-TEXT QUANTITIES:\n"
                "  pypdf corrupts qty values by merging them with adjacent column digits.\n"
                "  Two known failure patterns in this drawing:\n"
                "    Pattern A — digit APPENDED: qty=14 beside part=3002619\n"
                "      → raw text shows '143' (the '3' from the part number is glued on)\n"
                "    Pattern B — digit DROPPED: qty=134\n"
                "      → raw text shows '13' (the '4' is displaced to another position)\n"
                "  RULE: Treat every qty value in the raw text as UNRELIABLE.\n"
                "  For EVERY row without exception: locate the QTY cell in the IMAGE\n"
                "  and read the number directly from there. That is the only correct source.\n\n"
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
        "  - Is manufacturer a short company name? (Good) Or a long spec string? (Wrong — you swapped columns)\n"
        "  - Is qty a small integer (1-500)? (Good) Or a long alphanumeric string? (Wrong — you put a part number in qty)\n"
        "  - Is item_num a single integer matching the NO. column? (Good) Or 0 for a sub-item row? (Good)\n\n"

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
        f"Set rows_extracted to the actual count of items you return.\n"
        f"The BOM has {total_rows} numbered rows. You may return MORE than {total_rows} if there are\n"
        "un-numbered sub-item or accessory rows — include every physical row in the table.\n"
        "For sub-items, use item_num:0 (or leave it blank). Do NOT skip them.\n"
        "If rows_extracted < " + str(total_rows) + ", you missed numbered rows — go back and find them."
    )

    # Thinking budget: Stage 2 is transcription, not reasoning — keep it lean.
    # 300 tokens per row is ample; hard cap at 32 000 so combined total stays
    # well under the 64 000 model limit even for very large BOMs.
    think_budget = max(12000, total_rows * 300)
    think_budget = min(think_budget, 32000)
    # Output budget: ~250 tokens per row covers JSON + all fields comfortably.
    max_out = max(12000, total_rows * 250)
    max_out = min(max_out, 48000)

    print(f"SCAN Stage 2: Extracting {total_rows} rows "
          f"(thinking={think_budget}, max_tokens={max_out}, "
          f"images={'yes' if bom_images else 'no'})", flush=True)
    result = _call_claude(claude_client, pdf_b64, prompt,
                          thinking_budget=think_budget, max_tokens=max_out,
                          stage_label="Stage2", images_b64=bom_images)

    items = result.get("bom_line_items", [])
    rows_reported = result.get("rows_extracted", len(items))
    print(f"SCAN Stage 2: Got {len(items)} items (model reported {rows_reported})", flush=True)
    # Diagnostic: print ALL rows so we can verify item_num/qty for every item
    for _di, _it in enumerate(items):
        print(f"  [Stage2] row[{_di+1:02d}] "
              f"item_num={str(_it.get('item_num','?')):>4}  "
              f"qty={str(_it.get('qty','?')):>5}  "
              f"pn={_it.get('part_number','')[:30]}",
              flush=True)

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
                "The BOM table is provided as high-resolution image tile(s). Each BOM page\n"
                "has been split into overlapping vertical tiles for maximum character clarity.\n"
                "The tiles are in order from top to bottom.\n"
                "Verify each part number against what you see in these images, character by\n"
                "character. Pay close attention to similar-looking SHX font characters:\n"
                "D↔O, 3↔5, C↔O, 0↔O, 6↔8, B↔8, 1↔I.\n\n"
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
            "=== OCR / RAW TEXT FROM PDF (REFERENCE) ===\n"
            "The following text was extracted from the BOM (either from the PDF text layer\n"
            "or via pytesseract OCR on the rendered image).\n"
            "Use this text for STRUCTURE: number of characters, hyphens, format of each PN.\n"
            "Then verify EACH CHARACTER against the high-res image, especially for SHX\n"
            "font characters that OCR commonly confuses:\n"
            "  D↔O (both oval), 3↔5, S↔5 (nearly identical!), 0↔O, C↔O, 6↔8, B↔8, 1↔I\n"
            "If the image CLEARLY shows a different character than OCR, use the image.\n"
            "NEVER rearrange or transpose character ORDER — keep OCR's sequence.\n"
            "Check trailing characters carefully — OCR often drops the last 1-2 chars.\n\n"
            f"{bom_raw_text}\n\n"
            "=== END RAW TEXT ===\n\n"
        )

    prompt = (
        "I extracted these part numbers from the BOM table in this drawing PDF.\n"
        f"{page_note}"
        f"{raw_text_section}"
        "Your job: verify each part number against the OCR text AND the high-res image.\n\n"
        "=== VERIFICATION STRATEGY ===\n"
        "IMPORTANT: Stage 2 already read these part numbers carefully from high-res\n"
        "image tiles. Your job is to CONFIRM they are correct, not to second-guess them.\n"
        "Only correct a part number if you are CERTAIN it is wrong.\n\n"
        "1. Start with the OCR text — it gives you the correct character COUNT, hyphens,\n"
        "   slashes, and overall structure of each part number.\n"
        "2. SHX font characters that look similar:\n"
        "   D↔O, 3↔5, S↔5 (nearly identical!), 0↔O, C↔O, 6↔8, B↔8, 1↔I\n"
        "   USE CONTEXT to disambiguate:\n"
        "     - Letter sequence like BOOS → use S (not 5), use O (not 0)\n"
        "     - Digit sequence like 1800 → use 0 (not O), use 8 (not B)\n"
        "     - Mixed like IR18219K → digits in the middle, letter at end\n"
        "   Do NOT replace O with 0 in letter sequences (BOOS ≠ B005).\n"
        "   Do NOT replace S with 5 in letter sequences (BOOS ≠ BOO5).\n"
        "3. NEVER transpose or rearrange character ORDER. If it says IR18219K,\n"
        "   do NOT change it to IR1821K9 — keep the sequence.\n"
        "4. Check TRAILING characters — OCR drops chars near column borders.\n"
        "   If the image shows an extra trailing char, ADD it.\n"
        "5. Formatting fixes: em-dashes (—) → hyphens (-),\n"
        "   double-dashes (--) → single dashes (-), extra spaces removed.\n\n"
        "EXTRACTED PART NUMBERS:\n"
        f"{pn_list}\n\n"
        "MANDATORY FIXES — apply without re-examining the image:\n"
        "  double dash → single dash  — e.g. AB--CD → AB-CD\n"
        "  extra spaces inside a PN  — remove them\n\n"
        "For EACH part number above:\n"
        "1. Find it in the actual BOM table in the PDF"
    )
    if bom_raw_text.strip():
        prompt += " AND cross-check against the raw text above"
    prompt += (
        "\n"
        "2. Cross-check the extracted PN against the high-res image.\n"
        "   Use context to disambiguate ambiguous SHX characters:\n"
        "   - Letter sequences (BOOS, AUTO, MODE) → keep as letters. Do NOT replace O→0 or S→5.\n"
        "   - Digit sequences (1800, 18219) → keep as digits. Do NOT replace 0→O or 5→S.\n"
        "   - Check the LAST 1-2 characters — OCR drops trailing chars near column borders.\n"
        "3. NEVER transpose or rearrange character ORDER from the extracted PN.\n"
        "   If it says IR18219K, do NOT change it to IR1821K9.\n"
        "4. Formatting fixes: em-dash (—) → hyphen (-), double-dash → single, remove spaces.\n"
        "5. If the extracted PN looks correct in context, mark it VERIFIED. Prefer verifying\n"
        "   over correcting — only correct if you are CERTAIN a character is wrong.\n"
        "6. If a character is genuinely wrong per the image AND context, provide the CORRECT value.\n"
        "7. If you cannot find it in the PDF at all, mark it as not_found.\n\n"
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

        # Guard: Stage 1 found the table but returned 0 rows — non-deterministic
        # Claude failure. Retry once with a higher thinking budget to get a real count.
        if bom_count > 0 and row_count == 0:
            print(f"SCAN: Stage 1 found {bom_count} BOM table(s) but 0 rows — "
                  f"retrying Stage 1 with higher thinking budget...", flush=True)
            try:
                structure2 = _stage1_detect_structure(claude_client, pdf_b64,
                                                       thinking_budget=20000)
                row_count2 = structure2.get("total_bom_rows", 0)
                if row_count2 > 0:
                    print(f"SCAN: Stage 1 retry succeeded — {row_count2} rows found", flush=True)
                    # Merge pages_with_bom: take union of original + retry so we never
                    # lose a BOM page that one call identified but the other missed.
                    orig_pages = structure.get("pages_with_bom", [])
                    retry_pages = structure2.get("pages_with_bom", [])
                    merged_pages = sorted(set(orig_pages) | set(retry_pages))
                    structure = structure2
                    if merged_pages:
                        structure["pages_with_bom"] = merged_pages
                        print(f"SCAN: pages_with_bom merged: orig={orig_pages} "
                              f"retry={retry_pages} → {merged_pages}", flush=True)
                    row_count = row_count2
                    bom_count = structure2.get("bom_tables_found", bom_count)
                else:
                    # Still 0 after retry — use fallback row count so the pipeline
                    # can continue rather than silently skipping the whole BOM
                    fallback_rows = 50
                    print(f"SCAN: Stage 1 retry still returned 0 rows — "
                          f"using fallback row_count={fallback_rows} to continue", flush=True)
                    structure["total_bom_rows"] = fallback_rows
                    row_count = fallback_rows
            except Exception as _retry_err:
                print(f"SCAN: Stage 1 retry failed ({_retry_err}) — "
                      f"using fallback row_count=50", flush=True)
                structure["total_bom_rows"] = 50
                row_count = 50

        if bom_count == 0:
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

        # == PROGRAMMATIC BOM PAGE DETECTION (v2.7) ============================
        # Stage 1 (Claude) may miss continuation BOM pages. Run a fast
        # programmatic scan with fitz to catch any pages Claude missed.
        bom_pages = structure.get("pages_with_bom", [])
        orig_bom_pages = list(bom_pages)  # save original for comparison
        try:
            prog_pages = _detect_bom_pages_programmatic(pdf_b64,
                                                         hint_pages=bom_pages)
            if prog_pages:
                merged = sorted(set(bom_pages) | set(prog_pages))
                if merged != sorted(bom_pages):
                    print(f"SCAN [{filename}]: Programmatic BOM detection found "
                          f"additional pages: Stage1={bom_pages} + "
                          f"programmatic={prog_pages} → merged={merged}", flush=True)
                    bom_pages = merged
                    structure["pages_with_bom"] = merged
                    # Update row count estimate if we gained pages
                    if len(merged) > len(orig_bom_pages):
                        # Rough estimate: ~40 rows per BOM page
                        new_estimate = max(row_count, len(merged) * 40)
                        if new_estimate > row_count:
                            print(f"SCAN [{filename}]: Adjusting row estimate "
                                  f"{row_count} → {new_estimate} for "
                                  f"{len(merged)} BOM pages", flush=True)
                            structure["total_bom_rows"] = new_estimate
                            row_count = new_estimate
        except Exception as _prog_err:
            print(f"SCAN [{filename}]: Programmatic BOM detection failed: "
                  f"{_prog_err}", flush=True)

        # == EXTRACT BOM PAGES (THE KEY FIX) ==================================
        # Instead of sending the full 35-page drawing to Stage 2 & 5, extract
        # ONLY the BOM page(s) into a small PDF. This eliminates the noise from
        # schematics, wiring diagrams, etc. that was causing hallucination.
        bom_extracted = False
        bom_raw_text = ""
        bom_images = None  # v2.5: rendered PNG images for Stages 2 & 5
        if bom_pages:
            bom_pdf_b64, bom_raw_text, page_text_source = _extract_pages(pdf_b64, bom_pages)
            # Check if extraction actually produced a different (smaller) PDF
            bom_extracted = (bom_pdf_b64 != pdf_b64)
            if bom_extracted:
                # Signal to Stage 2 that page numbers no longer apply —
                # the extracted PDF contains ONLY BOM pages starting at page 1
                structure["_bom_extracted"] = True
                structure["_bom_raw_text"] = bom_raw_text
                # "columns" = fitz word-bbox table (best), "pypdf" = plain extract_text()
                # May be overwritten to "ocr" below for SHX/vector-font drawings
                structure["_bom_text_source"] = page_text_source
                print(f"SCAN [{filename}]: BOM pages extracted successfully "
                      f"(raw text: {len(bom_raw_text)} chars)", flush=True)

                # v2.5: Render BOM pages to high-res PNG images
                # This is THE FIX — bypasses all PDF font/resource issues
                bom_images = _render_pdf_to_image(bom_pdf_b64, dpi=400)
                if bom_images:
                    print(f"SCAN [{filename}]: Rendered {len(bom_images)} BOM page(s) "
                          f"to 400 DPI PNG — Stage 2 & 5 will use IMAGES", flush=True)

                    # v2.6: OCR the rendered images to get character-accurate text.
                    # ONLY use OCR when pypdf extracted very little text — that
                    # indicates an AutoCAD SHX / vector-font drawing with no real
                    # text layer (e.g. ABB XIO-00).  If pypdf already got good
                    # text keep it — OCR on normal PDFs scrambles table structure.
                    #
                    # Threshold is row-count-aware: a real text-layer BOM should
                    # have at least ~20 chars per row (part number + description
                    # fragment).  If chars_per_row < 20, the BOM text wasn't
                    # extracted even though some page text exists (title block /
                    # notes).  Example: 85-row BOM with 457 chars = 5 chars/row
                    # → the BOM is image-only, OCR is needed.
                    pypdf_chars = len(bom_raw_text.strip())
                    expected_min_chars = max(300, row_count * 20)
                    if pypdf_chars < expected_min_chars:
                        # SHX / vector-font drawing (e.g. ABB XIO-00): run OCR to
                        # get character-accurate text AND crop the image tightly to
                        # the BOM table so Claude sees it at full resolution.
                        ocr_text, bom_images_cropped = _ocr_images(bom_images)
                        # v2.8: Re-render BOM crop at 400 DPI, enhance strokes,
                        # and TILE into small sections. Claude's vision downscales
                        # large images, so one 3200x6600 image → only 11px/char.
                        # Tiling into ~1500px sections → 23px/char (2× improvement).
                        # Stroke enhancement thickens thin SHX lines for better D/O,
                        # 3/5, C/O distinction.
                        if bom_images_cropped:
                            try:
                                hires = _render_bom_crops_hires_auto(
                                    bom_pdf_b64, bom_images, bom_images_cropped
                                )
                                if hires:
                                    bom_images = hires
                                    print(f"SCAN [{filename}]: Using TILED BOM crops "
                                          f"({len(hires)} tiles, enhanced) for Stage 2/5",
                                          flush=True)
                                else:
                                    bom_images = bom_images_cropped
                                    print(f"SCAN [{filename}]: HiRes failed, using OCR crops "
                                          f"({len(bom_images_cropped)} page(s)) for Stage 2/5",
                                          flush=True)
                            except Exception as _hr_err:
                                bom_images = bom_images_cropped
                                print(f"SCAN [{filename}]: HiRes error ({_hr_err}), "
                                      f"using OCR crops", flush=True)
                        # Free intermediate images no longer needed
                        del bom_images_cropped
                        import gc; gc.collect()
                        if ocr_text.strip():
                            structure["_bom_raw_text"] = ocr_text
                            # v2.7: ALWAYS use "ocr" mode for OCR-sourced text, even
                            # if it produced a column-aware pipe table.  "columns" mode
                            # tells Claude to trust part numbers verbatim, but OCR on
                            # SHX/vector fonts misreads similar characters (3→5, D→O,
                            # 0→O, B→8).  "ocr" mode tells Claude to use the IMAGE
                            # as authority and only use OCR text as a structural guide.
                            structure["_bom_text_source"] = "ocr"
                            first_line = ocr_text.split("\n")[0] if ocr_text else ""
                            if first_line.count(" | ") >= 2:
                                print(f"SCAN [{filename}]: OCR produced column-aware table "
                                      f"({len(ocr_text)} chars) — using 'ocr' mode (image "
                                      f"is authority for part numbers)", flush=True)
                            print(f"SCAN [{filename}]: pypdf had only {pypdf_chars} chars "
                                  f"(SHX/vector font) — using OCR text ({len(ocr_text)} chars) "
                                  f"as structural guide", flush=True)
                        else:
                            print(f"SCAN [{filename}]: OCR returned no text "
                                  f"— Stage 2 will rely on image only", flush=True)
                    else:
                        # Good pypdf text — skip OCR entirely to avoid OOM on large
                        # drawings.  pytesseract + 5 preprocessing passes on a 200 MB
                        # image is what triggers Railway's SIGKILL.
                        print(f"SCAN [{filename}]: pypdf text sufficient "
                              f"({pypdf_chars} chars >= {expected_min_chars} min for "
                              f"{row_count} rows) — skipping OCR to conserve memory",
                              flush=True)
                else:
                    # Image rendering failed — fall back to FULL original PDF.
                    # The page-extracted PDF has broken font resources (CAD PDFs
                    # embed fonts globally; extracting a single page strips them).
                    # The full PDF has all fonts intact and Claude can read it.
                    print(f"SCAN [{filename}]: Image rendering failed, "
                          f"falling back to FULL PDF (font-safe)", flush=True)
                    bom_pdf_b64 = pdf_b64
                    structure["_bom_extracted"] = False
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
        # v2.7: Use BOM-only PDF (not full drawing) — cuts input from 5MB to
        # ~1.6MB, saving 30-60s on the API call and preventing worker timeouts.
        # Stage 3 is classification work based on the BOM data (already extracted
        # as text in the prompt); the PDF is just for terminal/wire schedules
        # which are on the BOM pages too.
        stage3_pdf = bom_pdf_b64 if bom_extracted else pdf_b64
        t3 = time.time()
        print(f"SCAN [{filename}]: Starting Stage 3 -- quantities "
              f"(using {'BOM PDF' if bom_extracted else 'full PDF'})", flush=True)
        try:
            qty_result = _stage3_derive_quantities(claude_client, stage3_pdf, bom_items)
        except Exception as e3:
            err_str = str(e3)
            if "429" in err_str or "rate_limit" in err_str.lower():
                print(f"SCAN: Rate limit on Stage 3, waiting 30s...", flush=True)
                time.sleep(30)
                try:
                    qty_result = _stage3_derive_quantities(claude_client, stage3_pdf, bom_items)
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
