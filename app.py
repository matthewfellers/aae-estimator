import os, json, base64, re
from flask import Flask, render_template, request, jsonify, send_file, send_from_directory
from anthropic import Anthropic
try:
    from supabase import create_client
except Exception:
    create_client = None
import io
from datetime import datetime
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT

app = Flask(__name__)

# Clients
claude_client = Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
_sb_url = os.environ.get("SUPABASE_URL", "")
_sb_key = os.environ.get("SUPABASE_ANON_KEY", "")
try:
    supabase = create_client(_sb_url, _sb_key) if (create_client and _sb_url and _sb_key) else None
except Exception as e:
    print(f"Supabase init failed (continuing without DB): {e}")
    supabase = None

# ── Labor rates (minutes per unit) ────────────────────────────────────────
# Labor rates in MINUTES per unit — calibrated against AAE actual job history
# Baseline: CR Permian PLC panel = 28 hrs actual
LABOR_RATES = {
    # Enclosure & mechanical
    "enclosure_prep": 20, "subpanel_mount": 15, "panel_layout": 25,
    "din_rail": 6, "wire_duct": 4, "enc_accessory": 8, "door_component": 12,
    # Power distribution
    "main_breaker_small": 15, "main_breaker_large": 30,
    "branch_breaker_1p": 6, "branch_breaker_23p": 10,
    "fused_disconnect": 15, "cpt": 20, "pdb": 12,
    # Motor control
    "relay_icecube": 8, "relay_din": 6, "contactor_small": 15,
    "contactor_large": 25, "overload": 10, "timer": 8, "ssr": 10,
    # VFDs / soft starters
    "vfd_small": 40, "vfd_med": 65, "vfd_large": 100,
    "soft_starter_small": 50, "soft_starter_large": 80,
    # Control devices
    "pilot_light": 6, "selector": 8, "pushbutton": 6, "estop": 12,
    # PLC / networking
    "plc_rack": 35, "plc_di_do": 2.5, "plc_ai_ao": 3.5, "hmi": 30,
    "safety_relay": 20, "eth_switch": 15, "eth_cable": 5,
    # Terminal blocks
    "tb_standard": 2.5, "tb_ground": 2.5, "tb_fused": 4, "tb_disconnect": 4,
    "tb_accessories": 3, "terminal_markers": 3,
    # Wiring (per wire, includes routing + landing both ends)
    "wire_land_control": 1.2, "ferrule": 0.25, "wire_route": 0.35,
    "heat_shrink_label": 0.25, "heat_shrink_batch": 2,
    # UL / QC (realistic shop times)
    "ul_labels": 15, "continuity_check": 0.4, "hipot": 12,
    "as_built": 25, "qc_signoff": 12,
}

COMPLEXITY_MULT = {
    "SIMPLE": 0.85, "STANDARD": 1.0, "MULTI": 1.1,
    "PLC-MOD": 1.15, "PLC-HIGH": 1.25, "PLC-DRIV": 1.35,
    "SAFETY": 1.4, "CUSTOM": 1.5,
}
TECH_MULT = {
    "MASTER": 0.85, "JOURNEYMAN": 1.0, "APPRENTICE": 1.3, "MIXED": 1.1,
}
SHOP_RATES = {
    "labor_rate": 95.0, "mat_markup": 0.30,
    "overhead": 0.12, "profit": 0.18, "expedite": 0.15,
    # Wire pricing per foot (AAE actual cost)
    "wire_10awg_under": 0.40, "wire_8awg": 0.65,
    "wire_6awg": 1.10, "wire_4awg": 1.75, "wire_2awg": 3.00,
    "wire_1awg": 3.50, "wire_1_0": 5.25, "wire_2_0": 5.50,
    "wire_3_0": 6.36, "wire_4_0": 7.50, "wire_250mcm": 13.00,
    # Heat shrink: H075X044H1T — $275/roll, 1000 labels, ~850 usable (2 rows wasted)
    "heat_shrink_roll_cost": 275.0,
    "heat_shrink_per_roll": 850,   # usable labels per roll
    # Legacy fallbacks
    "wire_16_per_ft": 0.40, "wire_14_per_ft": 0.40,
    "wire_12_per_ft": 0.40, "wire_10_per_ft": 0.40,
    "heat_shrink_each": round(275.0 / 850, 4),
}

# Wire gauge cost lookup (value from select = cost per foot)
WIRE_GAUGE_COSTS = {
    "10_under": 0.40, "8": 0.65, "6": 1.10, "4": 1.75,
    "2": 3.00, "1": 3.50, "1_0": 5.25, "2_0": 5.50,
    "3_0": 6.36, "4_0": 7.50, "250mcm": 13.00,
}

# ── Calculation engine ─────────────────────────────────────────────────────
def get_labor_rates():
    """Load labor rates from DB if available, fall back to defaults."""
    if not supabase:
        return LABOR_RATES.copy()
    try:
        rows = supabase.table("aae_labor_rates").select("rate_key,rate_value").execute()
        if rows.data:
            merged = LABOR_RATES.copy()
            for row in rows.data:
                merged[row["rate_key"]] = float(row["rate_value"])
            return merged
    except Exception:
        pass
    return LABOR_RATES.copy()

def get_vendor_map():
    """Load manufacturer->vendor mapping from DB."""
    vendor_map = {
        # defaults if DB unavailable
        "allen bradley": "Rexel", "rockwell automation": "Rexel",
        "hammond power": "Rexel", "panduit": "Rexel", "mersen": "Rexel",
        "hoffman": "Rexel", "n-tron": "Rexel", "corning": "Rexel",
        "phoenix contact": "AWC", "rittal": "AWC", "hammond enclosures": "AWC",
        "siemens": "AWC", "solar shield": "AWC", "bussmann": "AWC",
        "marathon special products": "AWC", "tripp lite": "AWC",
        "turck": "A-Tech", "red lion": "A-Tech",
        "square d": "Graybar", "schneider electric": "Graybar",
        "cisco": "TD Synnex",
        "saginaw control engineering": "Saginaw", "sce": "Saginaw",
    }
    if not supabase:
        return vendor_map
    try:
        rows = supabase.table("aae_vendors").select("manufacturer,vendor_name").eq("active", True).execute()
        if rows.data:
            for row in rows.data:
                vendor_map[row["manufacturer"].lower()] = row["vendor_name"]
    except Exception:
        pass
    return vendor_map

def calculate_bid(data):
    r = get_labor_rates()
    enc_qty  = max(1, int(data.get("enc_qty", 1)))
    din_runs = int(data.get("din_rail_runs", 3))
    duct_runs= int(data.get("wire_duct_runs", 4))
    acc_qty  = int(data.get("enc_accessories", 0))
    br_1p    = int(data.get("branch_1p", 0))
    br_2p    = int(data.get("branch_2p", 0))
    br_3p    = int(data.get("branch_3p", 0))
    fused_d  = int(data.get("fused_disconnects", 0))
    cpt      = 1 if str(data.get("cpt_present","N")).upper()=="Y" else 0
    pdb      = int(data.get("pdb_qty", 0))
    main_amp = int(data.get("main_amp", 100))
    relay_ic = int(data.get("relay_icecube", 0))
    relay_dn = int(data.get("relay_din", 0))
    cont_sm  = int(data.get("contactor_small", 0))
    cont_lg  = int(data.get("contactor_large", 0))
    overload = int(data.get("overload", 0))
    timers   = int(data.get("timers", 0))
    ssrs     = int(data.get("ssrs", 0))
    pilots   = int(data.get("pilot_lights", 0))
    selectors= int(data.get("selectors", 0))
    pbs      = int(data.get("push_buttons", 0))
    estops   = int(data.get("estops", 0))
    vfd_sm   = int(data.get("vfd_small", 0))
    vfd_md   = int(data.get("vfd_med", 0))
    vfd_lg   = int(data.get("vfd_large", 0))
    ss_sm    = int(data.get("soft_starter_small", 0))
    ss_lg    = int(data.get("soft_starter_large", 0))
    plc_yn   = str(data.get("plc_present","N")).upper()=="Y"
    di       = int(data.get("plc_di", 0))
    do_pts   = int(data.get("plc_do", 0))
    ai       = int(data.get("plc_ai", 0))
    ao       = int(data.get("plc_ao", 0))
    hmi      = 1 if str(data.get("hmi_present","N")).upper()=="Y" else 0
    safe_r   = 1 if str(data.get("safety_relay","N")).upper()=="Y" else 0
    eth_sw   = 1 if str(data.get("eth_switch","N")).upper()=="Y" else 0
    eth_cab  = int(data.get("eth_cables", 0))
    tb_std   = int(data.get("tb_standard", 0))
    tb_gnd   = int(data.get("tb_ground", 0))
    tb_fsd   = int(data.get("tb_fused", 0))
    tb_dis   = int(data.get("tb_disconnect", 0))
    tb_total = tb_std + tb_gnd + tb_fsd + tb_dis
    markers  = str(data.get("terminal_markers","Y")).upper()=="Y"
    ferrules = str(data.get("ferrules","Y")).upper()=="Y"
    wire_cnt      = int(data.get("wire_count", 0))
    wire_len      = float(data.get("wire_avg_len", 24))
    wire_gauge_key= data.get("wire_gauge", "10_under")
    hs_yn         = str(data.get("heat_shrink","Y")).upper()=="Y"
    fat_hrs       = float(data.get("fat_hours", 0))
    eng_hrs       = float(data.get("eng_hours", 0))
    prog_hrs      = float(data.get("prog_hours", 0))
    comp_key      = data.get("complexity", "STANDARD")
    tech_key      = data.get("tech_level", "JOURNEYMAN")
    expedite      = str(data.get("expedite","N")).upper()=="Y"
    lr_override   = float(data.get("labor_rate_override", 0))
    # Markup/margin: margin_type="margin"|"markup", margin_value=decimal e.g. 0.25
    margin_type   = data.get("margin_type", "markup")
    margin_value  = float(data.get("margin_value", SHOP_RATES["mat_markup"]))
    # Calibration factor from frontend (learned from actual vs estimated hours)
    calib_factor  = float(data.get("calib_factor", 1.0))

    # If wire count not provided, estimate it
    # Revised formula: I/O points drive most wiring on PLC panels
    # tb_total drives field wiring, not all I/O have individual home-run wires
    if wire_cnt == 0:
        wire_cnt = int((di+do_pts)*0.8 + (ai+ao)*1.0 + tb_total*0.6 + (br_2p+br_3p)*2)
        wire_cnt = max(wire_cnt, enc_qty * 10)  # minimum 10 wires per enclosure

    # Labor minutes by section
    enc_min = (r["enclosure_prep"]+r["subpanel_mount"]+r["panel_layout"])*enc_qty + \
              r["din_rail"]*din_runs + r["wire_duct"]*duct_runs*4 + r["enc_accessory"]*acc_qty

    pwr_min = (r["main_breaker_small"] if main_amp<=100 else r["main_breaker_large"])*enc_qty + \
              r["branch_breaker_1p"]*br_1p + r["branch_breaker_23p"]*(br_2p+br_3p) + \
              r["fused_disconnect"]*fused_d + r["cpt"]*cpt + r["pdb"]*pdb

    mc_min  = r["relay_icecube"]*relay_ic + r["relay_din"]*relay_dn + \
              r["contactor_small"]*cont_sm + r["contactor_large"]*cont_lg + \
              r["overload"]*overload + r["timer"]*timers + r["ssr"]*ssrs + \
              r["vfd_small"]*vfd_sm + r["vfd_med"]*vfd_md + r["vfd_large"]*vfd_lg + \
              r["soft_starter_small"]*ss_sm + r["soft_starter_large"]*ss_lg

    cd_min  = r["pilot_light"]*pilots + r["selector"]*selectors + \
              r["pushbutton"]*pbs + r["estop"]*estops

    plc_min = (r["plc_rack"] if plc_yn else 0) + \
              r["plc_di_do"]*(di+do_pts) + r["plc_ai_ao"]*(ai+ao) + \
              r["hmi"]*hmi + r["safety_relay"]*safe_r + \
              r["eth_switch"]*eth_sw + r["eth_cable"]*eth_cab

    tb_min  = r["tb_standard"]*tb_std + r["tb_ground"]*tb_gnd + \
              r["tb_fused"]*tb_fsd + r["tb_disconnect"]*tb_dis + \
              (r["terminal_markers"]*max(1,int(tb_total*1.2/10)) if markers else 0) + \
              r["wire_land_control"]*wire_cnt*2 + \
              (r["ferrule"]*wire_cnt*2 if ferrules else 0) + \
              r["wire_route"]*wire_cnt

    hs_min  = (r["heat_shrink_label"]*wire_cnt*2 +
               r["heat_shrink_batch"]*max(1,int(wire_cnt*2/50)) if hs_yn else 0)

    ul_min  = r["ul_labels"]*enc_qty + r["continuity_check"]*wire_cnt + \
              r["hipot"]*enc_qty + r["as_built"]*enc_qty + r["qc_signoff"]*enc_qty

    raw_min = enc_min+pwr_min+mc_min+cd_min+plc_min+tb_min+hs_min+ul_min
    raw_hrs = raw_min / 60.0

    c_mult    = COMPLEXITY_MULT.get(comp_key, 1.0)
    t_mult    = TECH_MULT.get(tech_key, 1.0)
    # Apply calibration factor to raw hours (learned from actual vs estimated)
    total_hrs = raw_hrs * c_mult * t_mult * calib_factor + fat_hrs + eng_hrs + prog_hrs

    # Wire cost using AAE per-gauge pricing
    wire_cost_per_ft = WIRE_GAUGE_COSTS.get(wire_gauge_key, 0.40)
    ctrl_wire_ft  = int(wire_cnt * wire_len / 12 * 1.15)
    pwr_wire_ft   = int((br_1p + (br_2p+br_3p)*2) * 1.5)
    total_wire_ft = ctrl_wire_ft + pwr_wire_ft
    wire_cost     = total_wire_ft * wire_cost_per_ft

    # Heat shrink: H075X044H1T $275/roll, 1000 labels, 850 usable
    hs_labels_qty  = wire_cnt * 2 if hs_yn else 0
    hs_rolls_needed= max(0, -(-hs_labels_qty // int(SHOP_RATES["heat_shrink_per_roll"])))
    hs_cost        = hs_rolls_needed * SHOP_RATES["heat_shrink_roll_cost"] if hs_yn else 0

    tb_marker_strips = max(0, int(tb_total*1.2/10)) if markers else 0
    ferrule_bags  = max(0, int(wire_cnt*2/500)+1) if ferrules else 0
    din_sticks    = din_runs
    duct_sections = duct_runs * 2
    eth_cables_qty= eth_cab
    spare_tbs     = int(tb_std * 0.1)

    mat_cost = (
        wire_cost +
        hs_cost +
        tb_marker_strips * 2.50 +
        ferrule_bags * 28.00 +
        din_sticks   * 12.50 +
        din_runs*2   * 1.20 +
        duct_sections* 8.75 +
        eth_cables_qty * 8.00 +
        cpt * 12.00 +
        spare_tbs    * 1.10 +
        enc_qty      * 18.00
    )

    # Manual BOM items
    manual_items = data.get("manual_bom", [])
    manual_total = sum(float(i.get("qty",0))*float(i.get("unit_cost",0)) for i in manual_items)
    mat_cost += manual_total

    # Apply markup or margin per user selection
    eff_labor_rate = lr_override if lr_override > 0 else SHOP_RATES["labor_rate"]
    labor_cost = total_hrs * eff_labor_rate
    if margin_type == "margin":
        divisor = max(0.01, 1.0 - margin_value)
        mat_with_markup = mat_cost / divisor
    else:
        mat_with_markup = mat_cost * (1 + margin_value)

    subtotal      = labor_cost + mat_with_markup
    overhead_cost = subtotal * SHOP_RATES["overhead"]
    pre_profit    = subtotal + overhead_cost
    exp_cost      = pre_profit * SHOP_RATES["expedite"] if expedite else 0
    total_price   = pre_profit / (1 - SHOP_RATES["profit"]) + exp_cost

    return {
        "raw_hours": round(raw_hrs, 2),
        "complexity_mult": c_mult,
        "_rates_snapshot": r,   # exact rates used — passed to hours report so line items match
        "tech_mult": t_mult,
        "calib_factor": calib_factor,
        "total_hours": round(total_hrs, 2),
        # Full hour breakdown for transparency report
        "hour_breakdown": {
            "enclosure_hrs":    round(enc_min/60, 2),
            "power_hrs":        round(pwr_min/60, 2),
            "motor_ctrl_hrs":   round(mc_min/60, 2),
            "control_dev_hrs":  round(cd_min/60, 2),
            "plc_network_hrs":  round(plc_min/60, 2),
            "terminals_wire_hrs": round(tb_min/60, 2),
            "heat_shrink_hrs":  round(hs_min/60, 2),
            "ul_qc_hrs":        round(ul_min/60, 2),
            "fat_hrs":          fat_hrs,
            "eng_hrs":          eng_hrs,
            "prog_hrs":         prog_hrs,
            "wire_count_used":  wire_cnt,
            "raw_min_total":    round(raw_min, 1),
        },
        "wire_count": wire_cnt,
        "hs_labels_qty": hs_labels_qty,
        "hs_rolls": hs_rolls_needed,
        "ctrl_wire_ft": ctrl_wire_ft,
        "pwr_wire_ft": pwr_wire_ft,
        "wire_cost": round(wire_cost, 2),
        "hs_cost": round(hs_cost, 2),
        "wire_cost_per_ft": wire_cost_per_ft,
        "mat_cost_raw": round(mat_cost, 2),
        "mat_cost_markup": round(mat_with_markup, 2),
        "margin_type": margin_type,
        "margin_value": margin_value,
        "labor_cost": round(labor_cost, 2),
        "labor_rate_used": eff_labor_rate,
        "subtotal": round(subtotal, 2),
        "overhead_cost": round(overhead_cost, 2),
        "pre_profit": round(pre_profit, 2),
        "expedite_cost": round(exp_cost, 2),
        "total_price": round(total_price, 2),
        "price_per_hour": round(total_price/total_hrs, 2) if total_hrs > 0 else 0,
        "labor_pct": round(labor_cost/total_price*100, 1) if total_price > 0 else 0,
        "mat_pct": round(mat_with_markup/total_price*100, 1) if total_price > 0 else 0,
    }

# ── AI Drawing Scan ────────────────────────────────────────────────────────
def scan_drawing(pdf_b64, filename="drawing.pdf"):
    # Stage 1 + 2 combined: classify and extract in one smart call
    prompt = """You are an expert electrical estimator at AAE Automation, a UL-508A certified industrial control panel shop. Analyze this electrical drawing set and extract ALL component quantities needed for a panel bid.

Return ONLY valid JSON — no markdown, no explanation. Just the raw JSON object.

{
  "extraction_summary": {
    "drawing_types_found": ["BOM", "SCHEMATIC", "TERMINAL_SCHEDULE", "IO_LIST"],
    "confidence": 0.0,
    "scope_gap_flags": [],
    "review_flags": []
  },
  "quantities": {
    "enc_qty": 1, "din_rail_runs": 3, "wire_duct_runs": 4, "enc_accessories": 0,
    "main_amp": 100, "main_disconnect_type": "",
    "branch_1p": 0, "branch_2p": 0, "branch_3p": 0,
    "fused_disconnects": 0, "cpt_present": "N", "cpt_kva": 0, "pdb_qty": 0,
    "relay_icecube": 0, "relay_din": 0,
    "contactor_small": 0, "contactor_large": 0, "overload": 0,
    "timers": 0, "ssrs": 0,
    "pilot_lights": 0, "selectors": 0, "push_buttons": 0, "estops": 0,
    "vfd_small": 0, "vfd_med": 0, "vfd_large": 0,
    "soft_starter_small": 0, "soft_starter_large": 0,
    "plc_present": "N", "plc_manufacturer": "", "plc_model": "",
    "plc_di": 0, "plc_do": 0, "plc_ai": 0, "plc_ao": 0,
    "hmi_present": "N", "hmi_size": 0,
    "safety_relay": "N", "eth_switch": "N", "eth_cables": 0,
    "tb_standard": 0, "tb_ground": 0, "tb_fused": 0, "tb_disconnect": 0,
    "wire_count": 0, "wire_avg_len": 24
  },
  "bom_line_items": [
    {
      "item_num": 1,
      "part_number": "WM483610NC",
      "description": "Rittal Enclosure 48x36x10 NEMA 4",
      "qty": 1,
      "unit": "ea",
      "manufacturer": "RITTAL",
      "category": "Enclosure",
      "notes": ""
    }
  ]
}

Rules:
- VFDs: <=5HP = small, 6-25HP = med, 26-100HP = large
- Contactors: <=40A = small, >40A = large
- Soft starters: <=50A = small, >50A = large
- Count wire numbers if a wire schedule exists for wire_count
- If no wire schedule: estimate wire_count as (DI+DO)*0.8 + (AI+AO)*1.0 + terminals*0.6
- bom_line_items: extract EVERY line item from any BOM table found in the drawings
  - Include part numbers, descriptions, quantities, manufacturers exactly as shown
  - Use these categories: Enclosure, Power, Motor Ctrl, Control Devices, PLC/Network, Terminals, Relays, Wiring, HMI/Computer, Markers, Other
  - If no BOM table found, return empty array []
  - Set qty to numeric value (not "A/R" — use 1 for A/R items and note in notes field)
- Flag anything uncertain in review_flags
- confidence: 0.0 to 1.0"""

    import time

    # Try primary model first, fall back to haiku on rate limit
    models_to_try = ["claude-sonnet-4-20250514", "claude-haiku-4-5-20251001"]

    for attempt, model in enumerate(models_to_try):
        try:
            response = claude_client.messages.create(
                model=model,
                max_tokens=4000,
                messages=[{
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
            )
            raw = response.content[0].text.strip()
            # Strip markdown code fences
            raw = re.sub(r"^```json\s*", "", raw)
            raw = re.sub(r"^```\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw)
            raw = raw.strip()
            # Extract just the JSON object if there's surrounding text
            json_match = re.search(r'\{.*\}', raw, re.DOTALL)
            if json_match:
                raw = json_match.group(0)
            # Fix common AI JSON mistakes: trailing commas before } or ]
            raw = re.sub(r',\s*([}\]])', r'\1', raw)
            try:
                result = json.loads(raw)
            except json.JSONDecodeError as je:
                print(f"JSON parse error: {je} — attempting repair")
                start = raw.find('{')
                end = raw.rfind('}')
                if start != -1 and end != -1:
                    raw = raw[start:end+1]
                    raw = re.sub(r',\s*([}\]])', r'\1', raw)
                    result = json.loads(raw)
                else:
                    raise
            result["_model_used"] = model
            return result

        except Exception as e:
            err_str = str(e)
            print(f"SCAN attempt {attempt+1} ({model}) ERROR: {err_str}")

            # Rate limit — wait briefly and try fallback
            if "429" in err_str or "rate_limit" in err_str.lower() or "overloaded" in err_str.lower():
                if attempt < len(models_to_try) - 1:
                    time.sleep(2)
                    continue  # try next model
                # All models failed with rate limit
                return {
                    "error": "rate_limit",
                    "error_message": "API rate limit reached. Please wait 60 seconds and try again, or check your Anthropic account usage at console.anthropic.com.",
                    "quantities": {}, "bom_line_items": [],
                    "extraction_summary": {"confidence": 0, "scope_gap_flags": ["rate_limit"]}
                }

            # Other error — return detail
            import traceback
            err_detail = traceback.format_exc()
            print("SCAN ERROR DETAIL:", err_detail)
            return {"error": str(e), "error_detail": err_detail,
                    "quantities": {}, "bom_line_items": [],
                    "extraction_summary": {"confidence": 0}}

    return {"error": "all_models_failed", "quantities": {}, "bom_line_items": [],
            "extraction_summary": {"confidence": 0}}

# ── PDF Quote Generator ────────────────────────────────────────────────────
def generate_quote_pdf(bid_data, calc_results, quote_number):
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter,
                            rightMargin=0.75*inch, leftMargin=0.75*inch,
                            topMargin=0.75*inch, bottomMargin=0.75*inch)

    navy = colors.HexColor("#1F3864")
    blue = colors.HexColor("#2E74B5")
    orange = colors.HexColor("#C55A11")
    light_gray = colors.HexColor("#F2F2F2")
    pale_blue = colors.HexColor("#DEEAF1")

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle("title", fontSize=20, textColor=colors.white,
                                 fontName="Helvetica-Bold", alignment=TA_CENTER, spaceAfter=4)
    sub_style   = ParagraphStyle("sub", fontSize=10, textColor=colors.HexColor("#BDD7EE"),
                                 fontName="Helvetica", alignment=TA_CENTER)
    section_style=ParagraphStyle("section", fontSize=11, textColor=colors.white,
                                 fontName="Helvetica-Bold", alignment=TA_LEFT)
    label_style = ParagraphStyle("label", fontSize=10, textColor=navy,
                                 fontName="Helvetica-Bold")
    value_style = ParagraphStyle("value", fontSize=10, textColor=colors.black,
                                 fontName="Helvetica")
    total_style = ParagraphStyle("total", fontSize=14, textColor=colors.white,
                                 fontName="Helvetica-Bold", alignment=TA_RIGHT)
    note_style  = ParagraphStyle("note", fontSize=8, textColor=colors.HexColor("#595959"),
                                 fontName="Helvetica-Oblique")

    story = []

    # Header
    header_data = [[Paragraph("AAE AUTOMATION", title_style)],
                   [Paragraph("UL-NNNY  |  UL-508A Certified  |  Industrial Control Panel Specialists", sub_style)]]
    header_table = Table(header_data, colWidths=[7*inch])
    header_table.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,-1), navy),
        ("ROWBACKGROUNDS", (0,0), (-1,-1), [navy]),
        ("TOPPADDING", (0,0), (-1,-1), 12),
        ("BOTTOMPADDING", (0,0), (-1,-1), 8),
        ("LEFTPADDING", (0,0), (-1,-1), 16),
    ]))
    story.append(header_table)
    story.append(Spacer(1, 0.2*inch))

    # Quote info row
    info_data = [
        [Paragraph("<b>QUOTATION</b>", ParagraphStyle("q", fontSize=16, textColor=orange, fontName="Helvetica-Bold")),
         Paragraph(f"<b>Quote #:</b> {quote_number}<br/>"
                   f"<b>Date:</b> {datetime.now().strftime('%B %d, %Y')}<br/>"
                   f"<b>Valid For:</b> 30 Days",
                   ParagraphStyle("qi", fontSize=10, fontName="Helvetica", leading=16))],
    ]
    info_table = Table(info_data, colWidths=[3.5*inch, 3.5*inch])
    info_table.setStyle(TableStyle([
        ("VALIGN", (0,0), (-1,-1), "TOP"),
        ("TOPPADDING", (0,0), (-1,-1), 4),
    ]))
    story.append(info_table)
    story.append(Spacer(1, 0.1*inch))
    story.append(HRFlowable(width="100%", thickness=2, color=navy))
    story.append(Spacer(1, 0.1*inch))

    # Customer / Project info
    customer = bid_data.get("customer_name", "")
    project  = bid_data.get("project_name", "")
    estimator= bid_data.get("estimator_name", "")
    if customer or project:
        proj_data = [
            ["Customer:", customer, "Project / PO:", project],
            ["Estimator:", estimator, "Panel Type:", bid_data.get("complexity","STANDARD")],
        ]
        proj_table = Table(proj_data, colWidths=[1.2*inch, 2.3*inch, 1.2*inch, 2.3*inch])
        proj_table.setStyle(TableStyle([
            ("FONTNAME", (0,0), (-1,-1), "Helvetica"),
            ("FONTSIZE", (0,0), (-1,-1), 10),
            ("FONTNAME", (0,0), (0,-1), "Helvetica-Bold"),
            ("FONTNAME", (2,0), (2,-1), "Helvetica-Bold"),
            ("TEXTCOLOR", (0,0), (0,-1), navy),
            ("TEXTCOLOR", (2,0), (2,-1), navy),
            ("TOPPADDING", (0,0), (-1,-1), 4),
            ("BOTTOMPADDING", (0,0), (-1,-1), 4),
        ]))
        story.append(proj_table)
        story.append(Spacer(1, 0.15*inch))

    # Cost Summary section
    def section_hdr(text):
        t = Table([[Paragraph(text, section_style)]], colWidths=[7*inch])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,-1), blue),
            ("TOPPADDING", (0,0), (-1,-1), 6),
            ("BOTTOMPADDING", (0,0), (-1,-1), 6),
            ("LEFTPADDING", (0,0), (-1,-1), 10),
        ]))
        return t

    story.append(section_hdr("COST SUMMARY"))
    story.append(Spacer(1, 0.05*inch))

    c = calc_results
    cost_rows = [
        ["Total Billable Hours", f"{c['total_hours']:,.2f} hrs",
         f"({c['raw_hours']:,.2f} raw × {c['complexity_mult']}× complexity × {c['tech_mult']}× tech)"],
        ["Labor Rate", f"${c['labor_rate_used']:,.2f}/hr", ""],
        ["Labor Cost", f"${c['labor_cost']:,.2f}", ""],
        ["Material Cost (w/ markup)", f"${c['mat_cost_markup']:,.2f}", f"Raw: ${c['mat_cost_raw']:,.2f}"],
        ["Overhead", f"${c['overhead_cost']:,.2f}", f"{int(SHOP_RATES['overhead']*100)}%"],
    ]
    if c['expedite_cost'] > 0:
        cost_rows.append(["Expedite Surcharge", f"${c['expedite_cost']:,.2f}", "15%"])

    cost_table = Table(cost_rows, colWidths=[2.5*inch, 1.8*inch, 2.7*inch])
    cost_table.setStyle(TableStyle([
        ("FONTNAME", (0,0), (-1,-1), "Helvetica"),
        ("FONTSIZE", (0,0), (-1,-1), 10),
        ("FONTNAME", (0,0), (0,-1), "Helvetica-Bold"),
        ("TEXTCOLOR", (0,0), (0,-1), navy),
        ("TEXTCOLOR", (2,0), (2,-1), colors.HexColor("#595959")),
        ("FONTSIZE", (2,0), (2,-1), 9),
        ("ROWBACKGROUNDS", (0,0), (-1,-1), [colors.white, light_gray]),
        ("TOPPADDING", (0,0), (-1,-1), 6),
        ("BOTTOMPADDING", (0,0), (-1,-1), 6),
        ("LEFTPADDING", (0,0), (-1,-1), 8),
        ("LINEBELOW", (0,-1), (-1,-1), 1, navy),
    ]))
    story.append(cost_table)
    story.append(Spacer(1, 0.05*inch))

    # Total price box
    total_data = [[Paragraph(f"TOTAL QUOTED PRICE:  ${c['total_price']:,.2f}", total_style)]]
    total_table = Table(total_data, colWidths=[7*inch])
    total_table.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,-1), navy),
        ("TOPPADDING", (0,0), (-1,-1), 12),
        ("BOTTOMPADDING", (0,0), (-1,-1), 12),
        ("RIGHTPADDING", (0,0), (-1,-1), 16),
    ]))
    story.append(total_table)
    story.append(Spacer(1, 0.2*inch))

    # Sanity checks
    story.append(section_hdr("ESTIMATING METRICS"))
    story.append(Spacer(1, 0.05*inch))
    metrics = [
        ["Price Per Billable Hour", f"${c['price_per_hour']:,.2f}",
         "Labor %", f"{c['labor_pct']}%"],
        ["Heat Shrink Labels", f"{c['hs_labels_qty']:,} labels",
         "Material %", f"{c['mat_pct']}%"],
        ["Control Wire", f"{c['ctrl_wire_ft']:,} ft",
         "Power Wire", f"{c['pwr_wire_ft']:,} ft"],
        ["Est. Wire Count", f"{c['wire_count']:,} wires",
         "Wire Cost", f"${c['ctrl_wire_ft']*SHOP_RATES['wire_16_per_ft'] + c['pwr_wire_ft']*SHOP_RATES['wire_12_per_ft']:,.2f}"],
    ]
    m_table = Table(metrics, colWidths=[2*inch, 1.5*inch, 2*inch, 1.5*inch])
    m_table.setStyle(TableStyle([
        ("FONTNAME", (0,0), (-1,-1), "Helvetica"),
        ("FONTSIZE", (0,0), (-1,-1), 10),
        ("FONTNAME", (0,0), (0,-1), "Helvetica-Bold"),
        ("FONTNAME", (2,0), (2,-1), "Helvetica-Bold"),
        ("TEXTCOLOR", (0,0), (0,-1), navy),
        ("TEXTCOLOR", (2,0), (2,-1), navy),
        ("ROWBACKGROUNDS", (0,0), (-1,-1), [colors.white, light_gray]),
        ("TOPPADDING", (0,0), (-1,-1), 5),
        ("BOTTOMPADDING", (0,0), (-1,-1), 5),
        ("LEFTPADDING", (0,0), (-1,-1), 8),
    ]))
    story.append(m_table)
    story.append(Spacer(1, 0.2*inch))

    # Scope / notes
    story.append(section_hdr("SCOPE & TERMS"))
    story.append(Spacer(1, 0.05*inch))
    scope_items = [
        "Panel design and build per UL-508A standards",
        "UL labeling and NNNY certification included" if str(bid_data.get("ul_required","Y")).upper()=="Y" else "",
        "Engineering/design included" if str(bid_data.get("engineering","N")).upper()=="Y" else "Engineering/design NOT included",
        "Programming included" if str(bid_data.get("programming","N")).upper()=="Y" else "Programming NOT included",
        "Witness test included" if str(bid_data.get("witness_test","N")).upper()=="Y" else "",
        "Shipping NOT included — FOB our shop",
        "Quote valid for 30 days from date above",
        "Lead time TBD based on schedule at time of order",
    ]
    for item in scope_items:
        if item:
            story.append(Paragraph(f"• {item}", ParagraphStyle("scope", fontSize=10,
                         fontName="Helvetica", leftIndent=12, spaceAfter=3)))
    story.append(Spacer(1, 0.3*inch))
    story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor("#BFBFBF")))
    story.append(Spacer(1, 0.1*inch))
    story.append(Paragraph(
        "AAE Automation  |  UL-NNNY  |  UL-508A  |  This quotation is confidential and intended solely for the addressee.",
        ParagraphStyle("footer", fontSize=8, textColor=colors.HexColor("#595959"),
                       fontName="Helvetica", alignment=TA_CENTER)))

    doc.build(story)
    buffer.seek(0)
    return buffer

# ── Routes ─────────────────────────────────────────────────────────────────
@app.route("/static/<path:filename>")
def static_files(filename):
    return send_from_directory("static", filename)


@app.route("/api/test")
def api_test():
    """Quick health check — verifies Anthropic API key and model are working."""
    try:
        resp = claude_client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=20,
            messages=[{"role":"user","content":"Reply with the word OK only."}]
        )
        return jsonify({"status": "ok", "reply": resp.content[0].text,
                        "anthropic_sdk": "ok"})
    except Exception as e:
        import traceback
        return jsonify({"status": "error", "error": str(e),
                        "detail": traceback.format_exc()}), 500


@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/scan", methods=["POST"])
def scan():
    print("=== /api/scan called ===", flush=True)
    if "drawing" not in request.files:
        print("ERROR: No file in request", flush=True)
        return jsonify({"error": "No file uploaded"}), 400
    f = request.files["drawing"]
    print(f"File received: {f.filename}, size approx {len(f.read())} bytes", flush=True)
    f.seek(0)  # reset after read
    pdf_bytes = f.read()
    pdf_b64   = base64.standard_b64encode(pdf_bytes).decode("utf-8")
    api_key   = os.environ.get("ANTHROPIC_API_KEY", "")
    print(f"API key present: {bool(api_key)}, key prefix: {api_key[:8] if api_key else 'MISSING'}", flush=True)
    print(f"PDF b64 length: {len(pdf_b64)}", flush=True)
    result    = scan_drawing(pdf_b64, f.filename)
    print(f"Scan result keys: {list(result.keys())}", flush=True)
    if "error" in result:
        print(f"SCAN ERROR: {result['error']}", flush=True)
    return jsonify(result)

@app.route("/api/calculate", methods=["POST"])
def calculate():
    import traceback
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No JSON data received"}), 400
        result = calculate_bid(data)
        return jsonify(result)
    except Exception as e:
        err = traceback.format_exc()
        print("CALCULATE ERROR:", err, flush=True)
        return jsonify({"error": str(e), "detail": err}), 500

@app.route("/api/save_bid", methods=["POST"])
def save_bid():
    data = request.get_json()
    try:
        row = {
            "customer_name":  data.get("customer_name",""),
            "project_name":   data.get("project_name",""),
            "estimator_name": data.get("estimator_name",""),
            "complexity":     data.get("complexity","STANDARD"),
            "tech_level":     data.get("tech_level","JOURNEYMAN"),
            "total_hours":    data.get("calc",{}).get("total_hours",0),
            "total_price":    data.get("calc",{}).get("total_price",0),
            "bid_data":       json.dumps(data),
            "created_at":     datetime.now().isoformat(),
        }
        if not supabase: return jsonify({"error": "Supabase not configured"}), 500
        result = supabase.table("bids").insert(row).execute()
        return jsonify({"success": True, "id": result.data[0]["id"]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/bids", methods=["GET"])
def get_bids():
    try:
        if not supabase: return jsonify([])
        result = supabase.table("bids").select("*").order("created_at", desc=True).limit(50).execute()
        return jsonify(result.data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/quote_pdf", methods=["POST"])
def quote_pdf():
    data = request.get_json()
    bid_data  = data.get("bid_data", {})
    calc_data = data.get("calc", {})
    quote_num = f"AAE-{datetime.now().strftime('%Y%m%d')}-{str(datetime.now().microsecond)[:4]}"
    pdf_buf   = generate_quote_pdf(bid_data, calc_data, quote_num)
    return send_file(pdf_buf, mimetype="application/pdf",
                     as_attachment=True,
                     download_name=f"AAE_Quote_{quote_num}.pdf")

@app.route("/api/bom_pdf", methods=["POST"])
def bom_pdf():
    """Generate a BOM PDF for download."""
    data      = request.get_json()
    bid_data  = data.get("bid_data", {})
    calc_data = data.get("calc", {})
    quote_num = data.get("quote_num", f"AAE-{datetime.now().strftime('%Y%m%d')}")
    buf = generate_bom_pdf(bid_data, calc_data, quote_num)
    return send_file(buf, mimetype="application/pdf", as_attachment=True,
                     download_name=f"AAE_BOM_{quote_num}.pdf")


@app.route("/api/bom_excel", methods=["POST"])
def bom_excel():
    """Generate post-calculation BOM Excel using the same premium format as bom_from_scan.
    Builds BOM from calc data (includes wire + heat shrink). Uses vendor mapping.
    """
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from collections import defaultdict

    data      = request.get_json()
    bid_data  = data.get("bid_data", {})
    calc_data = data.get("calc", {})
    quote_num = data.get("quote_num", f"AAE-{datetime.now().strftime('%Y%m%d')}")
    customer  = bid_data.get("customer_name", "")
    project   = bid_data.get("project_name", "")

    # Build structured BOM items from bid + calc
    bom_items = build_bom_items(bid_data, calc_data)
    # Convert to scan-BOM format
    vendor_map = get_vendor_map()
    line_items = []
    for i, itm in enumerate(bom_items, 1):
        mfr    = itm.get("manufacturer", "")
        vendor = vendor_map.get(mfr.lower(), "")
        if not vendor:
            for key, vname in vendor_map.items():
                if key and (key in mfr.lower() or mfr.lower() in key):
                    vendor = vname; break
        line_items.append({
            "item_num":    i,
            "part_number": itm.get("part_num", ""),
            "description": itm.get("description", ""),
            "qty":         itm.get("qty", 1),
            "unit":        itm.get("unit", "ea"),
            "manufacturer": mfr,
            "vendor":      vendor,
            "aae_cost":    itm.get("unit_cost", 0.0),
            "notes":       "",
            "category":    itm.get("category", "Other"),
        })

    # ── Excel workbook ─────────────────────────────────────────────────────────
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Bill of Materials"

    RED="9B1B1B"; DARK_RED="6B0A0A"; WHITE="FFFFFF"; LIGHT_RED="FDECEA"; MID_GRAY="F5F0F0"; DARK="2C2C2C"; TEAL="00897A"

    def s(cell, bold=False, bg=None, fg=WHITE, sz=10, ha="left"):
        cell.font = Font(name="Arial", bold=bold, color=fg, size=sz)
        if bg: cell.fill = PatternFill("solid", fgColor=bg)
        cell.alignment = Alignment(horizontal=ha, vertical="center", wrap_text=False)

    thin = Side(style="thin", color="E0D0D0")
    bdr  = Border(bottom=thin, left=thin, right=thin, top=thin)

    # 10 columns: A=Item,B=Part#,C=Description,D=Qty,E=Unit,F=Manufacturer,G=Vendor,H=Unit Cost,I=Total Cost,J=Notes
    for col, w in [("A",8),("B",22),("C",48),("D",6),("E",6),("F",22),("G",14),("H",14),("I",14),("J",20)]:
        ws.column_dimensions[col].width = w

    # Row 1: Banner
    ws.merge_cells("A1:J1"); ws.row_dimensions[1].height = 14
    ws["A1"] = "AAE AUTOMATION, INC.  |  UL-NNNY  |  UL-508A Certified Industrial Control Panel Specialists"
    s(ws["A1"], bold=True, bg=RED, sz=11, ha="center")

    # Row 2: Title | Quote#
    ws.merge_cells("A2:E2"); ws.row_dimensions[2].height = 34
    ws["A2"] = "BILL OF MATERIALS"
    ws["A2"].font      = Font(name="Arial", bold=True, color=WHITE, size=18)
    ws["A2"].fill      = PatternFill("solid", fgColor=DARK_RED)
    ws["A2"].alignment = Alignment(horizontal="left", vertical="center", indent=1)
    ws.merge_cells("F2:J2")
    ws["F2"] = quote_num
    s(ws["F2"], bold=True, bg=DARK_RED, fg="F4A9A8", sz=12, ha="right")

    # Row 3: Customer | Project
    ws.row_dimensions[3].height = 18
    ws.merge_cells("A3:B3"); ws["A3"] = "Customer:"
    s(ws["A3"], bold=True, bg=LIGHT_RED, fg=DARK_RED, sz=9, ha="right")
    ws.merge_cells("C3:E3"); ws["C3"] = customer
    s(ws["C3"], bg=LIGHT_RED, fg=DARK, sz=10)
    ws["F3"] = "Project:"
    s(ws["F3"], bold=True, bg=LIGHT_RED, fg=DARK_RED, sz=9, ha="right")
    ws.merge_cells("G3:J3"); ws["G3"] = project
    s(ws["G3"], bg=LIGHT_RED, fg=DARK, sz=10)

    # Row 4: Date | Estimator
    ws.row_dimensions[4].height = 16
    ws.merge_cells("A4:B4"); ws["A4"] = "Date:"
    s(ws["A4"], bold=True, bg=MID_GRAY, fg=DARK_RED, sz=9, ha="right")
    ws.merge_cells("C4:E4"); ws["C4"] = datetime.now().strftime("%m/%d/%Y")
    s(ws["C4"], bg=MID_GRAY, fg=DARK, sz=9)
    ws["F4"] = "Estimator:"
    s(ws["F4"], bold=True, bg=MID_GRAY, fg=DARK_RED, sz=9, ha="right")
    ws.merge_cells("G4:J4"); ws["G4"] = bid_data.get("estimator_name", "AAE Automation")
    s(ws["G4"], bg=MID_GRAY, fg=DARK, sz=9)

    # Row 5: Internal notice
    ws.merge_cells("A5:J5"); ws.row_dimensions[5].height = 15
    ws["A5"] = "⚠  INTERNAL DOCUMENT ONLY — Not for Customer Distribution  ⚠"
    s(ws["A5"], bold=True, bg="FFF8E1", fg="CC6600", sz=9, ha="center")

    # Row 6: Column headers
    ws.row_dimensions[6].height = 20
    for ci, h in enumerate(["ITEM","PART NUMBER","DESCRIPTION","QTY","U/M","MANUFACTURER","VENDOR","UNIT COST","TOTAL COST","NOTES"], 1):
        c = ws.cell(row=6, column=ci, value=h)
        s(c, bold=True, bg=DARK, sz=9, ha="center")
        c.border = Border(bottom=Side(style="medium", color=RED))

    # Group by category
    grouped = defaultdict(list)
    cat_order = ["Enclosure","Power","Motor Ctrl","Control","PLC/Network","Terminals","Wiring","Other"]
    for item in line_items:
        cat = item.get("category", "Other")
        if cat not in cat_order: cat = "Other"
        grouped[cat].append(item)

    row = 7; item_counter = 0
    even_fill = PatternFill("solid", fgColor="FDF8F8")
    odd_fill  = PatternFill("solid", fgColor=WHITE)

    for cat in [c for c in cat_order if grouped[c]]:
        # Section header
        ws.merge_cells(f"A{row}:J{row}")
        hc = ws.cell(row=row, column=1, value=f"  {cat.upper()}")
        hc.font = Font(name="Arial", bold=True, color=WHITE, size=9)
        hc.fill = PatternFill("solid", fgColor=RED)
        hc.alignment = Alignment(horizontal="left", vertical="center", indent=1)
        ws.row_dimensions[row].height = 16
        row += 1

        for itm in grouped[cat]:
            item_counter += 1
            fill = even_fill if item_counter % 2 == 0 else odd_fill
            ws.row_dimensions[row].height = 15
            vals = [itm["item_num"], itm["part_number"], itm["description"],
                    itm["qty"], itm["unit"], itm["manufacturer"],
                    itm.get("vendor",""), itm["aae_cost"],
                    itm["qty"] * itm["aae_cost"],   # TOTAL COST = qty × unit cost
                    itm["notes"]]
            for ci, val in enumerate(vals, 1):
                c = ws.cell(row=row, column=ci, value=val)
                c.fill = fill; c.border = bdr
                c.font = Font(name="Arial", size=9, color=DARK)
                if ci in (1, 4):
                    c.alignment = Alignment(horizontal="center", vertical="center")
                elif ci == 7:  # Vendor — teal
                    c.font = Font(name="Arial", size=9, color=TEAL, bold=bool(val))
                    c.alignment = Alignment(horizontal="center", vertical="center")
                elif ci == 8:  # Unit Cost
                    c.alignment = Alignment(horizontal="right", vertical="center")
                    c.number_format = '"$"#,##0.000'
                    c.font = Font(name="Arial", size=9, color="555555")
                elif ci == 9:  # Total Cost — bold green
                    c.alignment = Alignment(horizontal="right", vertical="center")
                    c.number_format = '"$"#,##0.00'
                    c.font = Font(name="Arial", size=9, color="008800", bold=True)
                else:
                    c.alignment = Alignment(horizontal="left", vertical="center", wrap_text=(ci==3))
            row += 1
        row += 1  # spacer

    # Total row
    ws.merge_cells(f"A{row}:G{row}")
    tl = ws.cell(row=row, column=1, value="TOTAL MATERIAL COST (AAE Cost):")
    tl.font = Font(name="Arial", bold=True, color=DARK_RED, size=10)
    tl.fill = PatternFill("solid", fgColor=LIGHT_RED)
    tl.alignment = Alignment(horizontal="right", vertical="center")
    tv = ws.cell(row=row, column=9, value=f"=SUM(I7:I{row-1})")
    tv.font = Font(name="Arial", bold=True, color="008800", size=11)
    tv.number_format = '"$"#,##0.00'
    tv.fill = PatternFill("solid", fgColor=LIGHT_RED)
    tv.alignment = Alignment(horizontal="right", vertical="center")
    ws.cell(row=row, column=8).fill = PatternFill("solid", fgColor=LIGHT_RED)
    ws.cell(row=row, column=10).fill = PatternFill("solid", fgColor=LIGHT_RED)
    ws.row_dimensions[row].height = 20
    row += 2

    # Footer
    ws.merge_cells(f"A{row}:J{row}")
    ft = ws.cell(row=row, column=1,
        value="AAE Automation, Inc.  |  8528 SW 2nd St, Oklahoma City, OK 73128  |  405-210-1567  |  mfellers@aaeok.com")
    ft.font = Font(name="Arial", size=8, color="888080", italic=True)
    ft.alignment = Alignment(horizontal="center")

    ws.freeze_panes = "A7"
    ws.auto_filter.ref = f"A6:J{row-3}"
    ws.page_setup.orientation = "landscape"
    ws.page_setup.fitToPage = True; ws.page_setup.fitToWidth = 1
    ws.print_title_rows = "1:6"

    buf = io.BytesIO()
    wb.save(buf); buf.seek(0)
    return send_file(buf,
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                     as_attachment=True,
                     download_name=f"AAE_BOM_{project.replace(' ','_') or quote_num}.xlsx")

@app.route("/api/bid_quote_pdf/<int:bid_id>", methods=["GET"])
def bid_quote_pdf(bid_id):
    """Download quote PDF for a saved bid by ID."""
    try:
        if not supabase: return jsonify({"error": "Supabase not configured"}), 500
        result = supabase.table("bids").select("*").eq("id", bid_id).single().execute()
        bid = result.data
        if not bid:
            return jsonify({"error": "Bid not found"}), 404
        bid_data  = json.loads(bid.get("bid_data", "{}"))
        calc_data = bid_data.get("calc", {})
        actual_bid = {k: v for k, v in bid_data.items() if k != "calc"}
        actual_bid["customer_name"] = bid.get("customer_name", "")
        actual_bid["project_name"]  = bid.get("project_name", "")
        actual_bid["estimator_name"]= bid.get("estimator_name", "")
        quote_num = f"AAE-{bid.get('created_at','')[:10].replace('-','')}-{bid_id}"
        pdf_buf   = generate_quote_pdf(actual_bid, calc_data, quote_num)
        return send_file(pdf_buf, mimetype="application/pdf", as_attachment=True,
                         download_name=f"AAE_Quote_{quote_num}.pdf")
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def build_bom_items(bid_data, calc_data):
    """Build a structured BOM item list from bid + calc data."""
    items = []
    def add(cat, part, desc, qty, unit, unit_cost, mfr=""):
        if qty <= 0: return
        items.append({"category": cat, "part_num": part, "description": desc,
                       "qty": qty, "unit": unit, "unit_cost": unit_cost,
                       "total_cost": qty * unit_cost, "manufacturer": mfr})

    enc  = int(bid_data.get("enc_qty", 1))
    add("Enclosure", "ENCLOSURE", "Panel Enclosure", enc, "ea", 450)
    add("Enclosure", "DIN-RAIL", "DIN Rail (1m stick)", int(bid_data.get("din_rail_runs", 0)), "ea", 12.50)
    add("Enclosure", "WIRE-DUCT", "Wire Duct Section", int(bid_data.get("wire_duct_runs", 0))*2, "ea", 8.75)
    add("Power", "MAIN-DISC", "Main Disconnect/Breaker", 1 if int(bid_data.get("main_amp",0))>0 else 0, "ea", 180)
    add("Power", "BREAKER-1P", "1P Branch Circuit Breaker", int(bid_data.get("branch_1p", 0)), "ea", 25)
    add("Power", "BREAKER-2P", "2P Branch Circuit Breaker", int(bid_data.get("branch_2p", 0)), "ea", 45)
    add("Power", "BREAKER-3P", "3P Branch Circuit Breaker", int(bid_data.get("branch_3p", 0)), "ea", 65)
    add("Power", "FUSED-DISC", "Fused Disconnect", int(bid_data.get("fused_disconnects", 0)), "ea", 85)
    add("Power", "CPT", "Control Power Transformer", 1 if str(bid_data.get("cpt_present","N")).upper()=="Y" else 0, "ea", 220)
    add("Power", "PWR-DIST-BLK", "Power Distribution Block", int(bid_data.get("pdb_qty", 0)), "ea", 28)
    add("Motor Ctrl", "CONT-SM", "Contactor ≤40A", int(bid_data.get("contactor_small", 0)), "ea", 65)
    add("Motor Ctrl", "CONT-LG", "Contactor 41-100A", int(bid_data.get("contactor_large", 0)), "ea", 140)
    add("Motor Ctrl", "OVERLOAD", "Overload Relay", int(bid_data.get("overload", 0)), "ea", 45)
    add("Motor Ctrl", "VFD-SM", "VFD ≤5HP", int(bid_data.get("vfd_small", 0)), "ea", 380)
    add("Motor Ctrl", "VFD-MD", "VFD 6-25HP", int(bid_data.get("vfd_med", 0)), "ea", 680)
    add("Motor Ctrl", "VFD-LG", "VFD 26-100HP", int(bid_data.get("vfd_large", 0)), "ea", 1400)
    add("Motor Ctrl", "SS-SM", "Soft Starter ≤50A", int(bid_data.get("soft_starter_small", 0)), "ea", 280)
    add("Motor Ctrl", "SS-LG", "Soft Starter >50A", int(bid_data.get("soft_starter_large", 0)), "ea", 520)
    add("Control", "RELAY-IC", "Ice Cube Relay", int(bid_data.get("relay_icecube", 0)), "ea", 18)
    add("Control", "RELAY-DIN", "DIN Mount Relay", int(bid_data.get("relay_din", 0)), "ea", 28)
    add("Control", "SSR", "Solid State Relay", int(bid_data.get("ssrs", 0)), "ea", 45)
    add("Control", "TIMER", "Timer Relay", int(bid_data.get("timers", 0)), "ea", 55)
    add("Control", "PILOT", "Pilot Light", int(bid_data.get("pilot_lights", 0)), "ea", 22)
    add("Control", "SEL-SW", "Selector Switch", int(bid_data.get("selectors", 0)), "ea", 28)
    add("Control", "PUSH-BTN", "Push Button", int(bid_data.get("push_buttons", 0)), "ea", 22)
    add("Control", "E-STOP", "E-Stop Button", int(bid_data.get("estops", 0)), "ea", 48)
    if str(bid_data.get("plc_present","N")).upper() == "Y":
        add("PLC/Network", "PLC-CTRL", "PLC Controller", 1, "ea", 600)
    add("PLC/Network", "DI-PT", "Digital Input Point", int(bid_data.get("plc_di", 0)), "pt", 8)
    add("PLC/Network", "DO-PT", "Digital Output Point", int(bid_data.get("plc_do", 0)), "pt", 10)
    add("PLC/Network", "AI-PT", "Analog Input Point", int(bid_data.get("plc_ai", 0)), "pt", 18)
    add("PLC/Network", "AO-PT", "Analog Output Point", int(bid_data.get("plc_ao", 0)), "pt", 22)
    if str(bid_data.get("hmi_present","N")).upper() == "Y":
        add("PLC/Network", "HMI", "HMI Display Panel", 1, "ea", 800)
    if str(bid_data.get("safety_relay","N")).upper() == "Y":
        add("PLC/Network", "SAFETY-CTRL", "Safety Relay/Controller", 1, "ea", 450)
    if str(bid_data.get("eth_switch","N")).upper() == "Y":
        add("PLC/Network", "ETH-SW", "Ethernet Switch", 1, "ea", 180)
    add("PLC/Network", "ETH-CBL", "Internal Ethernet Cable", int(bid_data.get("eth_cables", 0)), "ea", 12)
    add("Terminals", "TB-STD", "Standard Terminal Block", int(bid_data.get("tb_standard", 0)), "ea", 3.50, "Phoenix Contact")
    add("Terminals", "TB-GND", "Ground Terminal Block", int(bid_data.get("tb_ground", 0)), "ea", 4.20, "Phoenix Contact")
    add("Terminals", "TB-FUSED", "Fused Terminal Block", int(bid_data.get("tb_fused", 0)), "ea", 8.50, "Phoenix Contact")
    add("Terminals", "TB-DISC", "Disconnect Terminal Block", int(bid_data.get("tb_disconnect", 0)), "ea", 9.80)
    # Wire
    ctrl_ft = calc_data.get("ctrl_wire_ft", 0)
    pwr_ft  = calc_data.get("pwr_wire_ft", 0)
    wrate   = calc_data.get("wire_cost_per_ft", 0.40)
    if ctrl_ft + pwr_ft > 0:
        add("Wiring", "WIRE", f"Control/Power Wire ({ctrl_ft+pwr_ft} ft total)", ctrl_ft + pwr_ft, "ft", wrate, "Panduit")
    # Heat shrink labels
    hs_rolls = calc_data.get("hs_rolls", 0)
    if hs_rolls > 0:
        add("Wiring", "H075X044H1T", f"Heat Shrink Labels — {calc_data.get('hs_labels_qty',0)} pcs ({hs_rolls} roll{'s' if hs_rolls>1 else ''})", hs_rolls, "roll", 275, "Rexel")
    return items


def generate_bom_pdf(bid_data, calc_results, quote_number):
    """Generate a standalone BOM PDF with AAE branding."""
    buffer = io.BytesIO()
    from reportlab.lib.pagesizes import landscape
    doc = SimpleDocTemplate(buffer, pagesize=landscape(letter),
                            rightMargin=0.5*inch, leftMargin=0.5*inch,
                            topMargin=0.5*inch, bottomMargin=0.5*inch)
    red   = colors.HexColor("#9B1B1B")
    dark  = colors.HexColor("#2C2C2C")
    lgray = colors.HexColor("#F0EDE8")
    styles = getSampleStyleSheet()

    story = []
    # Header
    hdr_data = [[Paragraph("<font color='white'><b>AAE AUTOMATION, INC.</b></font>",
                            ParagraphStyle("h", fontSize=16, fontName="Helvetica-Bold")),
                 Paragraph("<font color='white'>BILL OF MATERIALS</font>",
                            ParagraphStyle("h2", fontSize=12, fontName="Helvetica", alignment=TA_RIGHT))]]
    hdr_tbl = Table(hdr_data, colWidths=[5*inch, 4.5*inch])
    hdr_tbl.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,-1), red),
        ("TOPPADDING",(0,0),(-1,-1), 10), ("BOTTOMPADDING",(0,0),(-1,-1), 10),
        ("LEFTPADDING",(0,0),(-1,-1), 14), ("RIGHTPADDING",(0,0),(-1,-1), 14),
        ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
    ]))
    story.append(hdr_tbl)
    story.append(Spacer(1, 0.1*inch))

    # Sub-header info
    story.append(Paragraph(
        f"<b>Customer:</b> {bid_data.get('customer_name','')} &nbsp;&nbsp; "
        f"<b>Project:</b> {bid_data.get('project_name','')} &nbsp;&nbsp; "
        f"<b>Quote #:</b> {quote_number} &nbsp;&nbsp; "
        f"<b>Date:</b> {datetime.now().strftime('%m/%d/%Y')} &nbsp;&nbsp;&nbsp; "
        f"<font color='#CC6600'><b>⚠ INTERNAL DOCUMENT ONLY</b></font>",
        ParagraphStyle("sub", fontSize=9, fontName="Helvetica", spaceAfter=8)))
    story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor("#CCCCCC")))
    story.append(Spacer(1, 0.05*inch))

    # Column headers
    col_widths = [1.3*inch, 1.5*inch, 3.2*inch, 0.5*inch, 0.5*inch, 1*inch, 1*inch]
    col_heads  = [["Category","Part #","Description","Qty","Unit","Unit Cost","Total Cost"]]
    tbl_hdr = Table(col_heads, colWidths=col_widths)
    tbl_hdr.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,-1), dark),
        ("TEXTCOLOR",(0,0),(-1,-1), colors.white),
        ("FONTNAME",(0,0),(-1,-1),"Helvetica-Bold"),
        ("FONTSIZE",(0,0),(-1,-1), 8.5),
        ("TOPPADDING",(0,0),(-1,-1), 5), ("BOTTOMPADDING",(0,0),(-1,-1), 5),
        ("LEFTPADDING",(0,0),(-1,-1), 6),
    ]))
    story.append(tbl_hdr)

    # BOM rows
    bom_items = build_bom_items(bid_data, calc_results)
    last_cat  = None
    row_data  = []
    for item in bom_items:
        if item["category"] != last_cat:
            last_cat = item["category"]
            row_data.append([item["category"].upper(), "", "", "", "", "", ""])
        row_data.append([
            "", item["part_num"], item["description"],
            str(item["qty"]), item["unit"],
            f'${item["unit_cost"]:.2f}', f'${item["total_cost"]:.2f}'
        ])

    bom_tbl = Table(row_data, colWidths=col_widths)
    style_cmds = [
        ("FONTNAME",(0,0),(-1,-1),"Helvetica"),
        ("FONTSIZE",(0,0),(-1,-1), 8.5),
        ("TOPPADDING",(0,0),(-1,-1), 4), ("BOTTOMPADDING",(0,0),(-1,-1), 4),
        ("LEFTPADDING",(0,0),(-1,-1), 6),
        ("LINEBELOW",(0,0),(-1,-1), 0.5, colors.HexColor("#DDDDDD")),
        ("ALIGN",(3,0),(6,-1),"RIGHT"),
    ]
    # Category rows styling
    ri = 0
    for item in bom_items:
        if item["category"] != (bom_items[ri-1]["category"] if ri > 0 else None):
            style_cmds += [
                ("BACKGROUND",(0,ri),(6,ri), lgray),
                ("FONTNAME",(0,ri),(6,ri),"Helvetica-Bold"),
                ("TEXTCOLOR",(0,ri),(0,ri), red),
                ("SPAN",(0,ri),(6,ri)),
            ]
        elif ri % 2 == 0:
            style_cmds.append(("BACKGROUND",(0,ri),(6,ri), colors.HexColor("#FAFAFA")))
        ri += 1

    bom_tbl.setStyle(TableStyle(style_cmds))
    story.append(bom_tbl)

    # Total row
    total_raw = sum(i["total_cost"] for i in bom_items)
    story.append(Spacer(1, 0.1*inch))
    tot_data = [[Paragraph(f"<b>TOTAL MATERIAL COST (RAW): ${total_raw:,.2f}</b>",
                            ParagraphStyle("tot", fontSize=11, textColor=red, fontName="Helvetica-Bold", alignment=TA_RIGHT))]]
    story.append(Table(tot_data, colWidths=[9.5*inch]))
    doc.build(story)
    buffer.seek(0)
    return buffer


@app.route("/api/bom_from_scan", methods=["POST"])
def bom_from_scan():
    """Generate a formatted AAE BOM Excel from scanned line items."""
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

    data       = request.get_json()
    line_items = data.get("bom_line_items", [])
    customer   = data.get("customer_name", "")
    project    = data.get("project_name", "")
    job_num    = data.get("job_number", f"AAE-{datetime.now().strftime('%Y%m%d')}")

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Bill of Materials"

    # Column widths
    for col, w in [("A",10),("B",22),("C",54),("D",7),("E",7),("F",22),("G",14),("H",22)]:
        ws.column_dimensions[col].width = w

    RED      = "9B1B1B"; DARK_RED = "6B0A0A"; WHITE = "FFFFFF"
    LIGHT_RED= "FDECEA"; MID_GRAY = "F5F0F0"; DARK  = "2C2C2C"

    def s(cell, bold=False, bg=None, fg=WHITE, sz=10, ha="left", wrap=False):
        cell.font = Font(name="Arial", bold=bold, color=fg, size=sz)
        if bg: cell.fill = PatternFill("solid", fgColor=bg)
        cell.alignment = Alignment(horizontal=ha, vertical="center", wrap_text=wrap)

    thin = Side(style="thin", color="E0D0D0")
    bdr  = Border(bottom=thin, left=thin, right=thin, top=thin)

    # ── Column widths — 9 columns ──────────────────────────────────────────────
    for col, w in [("A",8),("B",22),("C",48),("D",6),("E",6),("F",22),("G",14),("H",12),("I",14),("J",20)]:
        ws.column_dimensions[col].width = w

    # ── Row 1: Banner (A1:I1) ──────────────────────────────────────────────────
    ws.merge_cells("A1:J1"); ws.row_dimensions[1].height = 14
    ws["A1"] = "AAE AUTOMATION, INC.  |  UL-NNNY  |  UL-508A Certified Industrial Control Panel Specialists"
    s(ws["A1"], bold=True, bg=RED, sz=11, ha="center")

    # ── Row 2: Title (A2:E2) | Job# (F2:I2) ───────────────────────────────────
    ws.merge_cells("A2:E2"); ws.row_dimensions[2].height = 34
    ws["A2"] = "BILL OF MATERIALS"
    ws["A2"].font      = Font(name="Arial", bold=True, color=WHITE, size=18)
    ws["A2"].fill      = PatternFill("solid", fgColor=DARK_RED)
    ws["A2"].alignment = Alignment(horizontal="left", vertical="center", indent=1)
    ws.merge_cells("F2:J2")
    ws["F2"] = job_num
    s(ws["F2"], bold=True, bg=DARK_RED, fg="F4A9A8", sz=12, ha="right")

    # ── Row 3: Customer (A3:B3 | C3:E3) | Project (F3 | G3:I3) ───────────────
    ws.row_dimensions[3].height = 18
    ws.merge_cells("A3:B3"); ws["A3"] = "Customer:"
    s(ws["A3"], bold=True, bg=LIGHT_RED, fg=DARK_RED, sz=9, ha="right")
    ws.merge_cells("C3:E3"); ws["C3"] = customer
    s(ws["C3"], bg=LIGHT_RED, fg=DARK, sz=10)
    ws["F3"] = "Project:"
    s(ws["F3"], bold=True, bg=LIGHT_RED, fg=DARK_RED, sz=9, ha="right")
    ws.merge_cells("G3:J3"); ws["G3"] = project
    s(ws["G3"], bg=LIGHT_RED, fg=DARK, sz=10)

    # ── Row 4: Date (A4:B4 | C4:E4) | Estimator (F4 | G4:I4) ─────────────────
    ws.row_dimensions[4].height = 16
    ws.merge_cells("A4:B4"); ws["A4"] = "Date:"
    s(ws["A4"], bold=True, bg=MID_GRAY, fg=DARK_RED, sz=9, ha="right")
    ws.merge_cells("C4:E4"); ws["C4"] = datetime.now().strftime("%m/%d/%Y")
    s(ws["C4"], bg=MID_GRAY, fg=DARK, sz=9)
    ws["F4"] = "Estimator:"
    s(ws["F4"], bold=True, bg=MID_GRAY, fg=DARK_RED, sz=9, ha="right")
    ws.merge_cells("G4:J4"); ws["G4"] = "AAE Automation"
    s(ws["G4"], bg=MID_GRAY, fg=DARK, sz=9)

    # ── Row 5: Internal notice (A5:I5) ────────────────────────────────────────
    ws.merge_cells("A5:J5"); ws.row_dimensions[5].height = 15
    ws["A5"] = "⚠  INTERNAL DOCUMENT ONLY — Not for Customer Distribution  ⚠"
    s(ws["A5"], bold=True, bg="FFF8E1", fg="CC6600", sz=9, ha="center")

    # ── Row 6: Column headers ──────────────────────────────────────────────────
    ws.row_dimensions[6].height = 20
    for ci, h in enumerate(["ITEM","PART NUMBER","DESCRIPTION","QTY","U/M","MANUFACTURER","VENDOR","UNIT COST","TOTAL COST","NOTES"], 1):
        c = ws.cell(row=6, column=ci, value=h)
        s(c, bold=True, bg=DARK, sz=9, ha="center")
        c.border = Border(bottom=Side(style="medium", color=RED))

    # ── Load vendor map ────────────────────────────────────────────────────────
    vendor_map = get_vendor_map()

    # Group items by category
    from collections import defaultdict
    grouped = defaultdict(list)
    cat_order = ["Enclosure","Power","Motor Ctrl","Control Devices","PLC/Network",
                 "Terminals","Relays","HMI/Computer","Wiring","Markers","Other"]
    for item in line_items:
        cat = item.get("category","Other")
        if cat not in cat_order: cat = "Other"
        grouped[cat].append(item)

    row = 7; item_counter = 0
    even_fill = PatternFill("solid", fgColor="FDF8F8")
    odd_fill  = PatternFill("solid", fgColor=WHITE)
    thin      = Side(style="thin", color="E0D0D0")
    bdr       = Border(bottom=thin, left=thin, right=thin, top=thin)
    TEAL      = "00897A"

    cats_with_items = [c for c in cat_order if grouped[c]]
    for cat in cats_with_items:
        items = grouped[cat]
        # Section header spanning 9 cols
        ws.merge_cells(f"A{row}:J{row}")
        hc = ws.cell(row=row, column=1, value=f"  {cat.upper()}")
        hc.font = Font(name="Arial", bold=True, color=WHITE, size=9)
        hc.fill = PatternFill("solid", fgColor=RED)
        hc.alignment = Alignment(horizontal="left", vertical="center", indent=1)
        ws.row_dimensions[row].height = 16
        row += 1

        for itm in items:
            item_counter += 1
            fill = even_fill if item_counter % 2 == 0 else odd_fill
            ws.row_dimensions[row].height = 15
            mfr    = itm.get("manufacturer", "")
            vendor = vendor_map.get(mfr.lower(), "")
            if not vendor:
                for key, vname in vendor_map.items():
                    if key and (key in mfr.lower() or mfr.lower() in key):
                        vendor = vname; break
            vals = [
                itm.get("item_num", item_counter),
                itm.get("part_number", ""),
                itm.get("description", ""),
                itm.get("qty", 1),
                itm.get("unit", "ea"),
                mfr,
                vendor,
                0.00,  # AAE cost — will pull from QB
                itm.get("notes", "")
            ]
            for ci, val in enumerate(vals, 1):
                c = ws.cell(row=row, column=ci, value=val)
                c.fill = fill; c.border = bdr
                c.font = Font(name="Arial", size=9, color=DARK)
                if ci in (1, 4):
                    c.alignment = Alignment(horizontal="center", vertical="center")
                elif ci == 7:  # Vendor — teal, centered
                    c.font = Font(name="Arial", size=9, color=TEAL, bold=bool(val))
                    c.alignment = Alignment(horizontal="center", vertical="center")
                elif ci == 8:  # AAE Cost
                    c.alignment = Alignment(horizontal="right", vertical="center")
                    c.number_format = '"$"#,##0.00'
                    c.font = Font(name="Arial", size=9, color="AAAAAA", italic=True)
                else:
                    c.alignment = Alignment(horizontal="left", vertical="center", wrap_text=(ci==3))
            row += 1

        row += 1  # spacer

    # Total row (spans A:G, value in H)
    ws.merge_cells(f"A{row}:H{row}")
    tl = ws.cell(row=row, column=1, value="TOTAL MATERIAL COST (pricing TBD from QuickBooks):")
    tl.font = Font(name="Arial", bold=True, color=DARK_RED, size=10)
    tl.fill = PatternFill("solid", fgColor=LIGHT_RED)
    tl.alignment = Alignment(horizontal="right", vertical="center")
    tv = ws.cell(row=row, column=9, value=f"=SUM(I7:I{row-1})")
    tv.font = Font(name="Arial", bold=True, color="AAAAAA", size=11, italic=True)
    tv.number_format = '"$"#,##0.00'
    tv.fill = PatternFill("solid", fgColor=LIGHT_RED)
    tv.alignment = Alignment(horizontal="right", vertical="center")
    ws.cell(row=row, column=8).fill = PatternFill("solid", fgColor=LIGHT_RED)
    ws.cell(row=row, column=10).fill = PatternFill("solid", fgColor=LIGHT_RED)
    ws.row_dimensions[row].height = 20
    row += 1

    # QB note
    ws.merge_cells(f"A{row}:J{row}")
    note = ws.cell(row=row, column=1,
        value="NOTE: AAE Cost column will be populated from QuickBooks pricing. Vendor column auto-assigned from AAE vendor database.")
    note.font = Font(name="Arial", size=8, color="888080", italic=True)
    note.alignment = Alignment(horizontal="left")
    row += 2

    # Footer
    ws.merge_cells(f"A{row}:J{row}")
    ft = ws.cell(row=row, column=1,
        value="AAE Automation, Inc.  |  8528 SW 2nd St, Oklahoma City, OK 73128  |  405-210-1567  |  mfellers@aaeok.com")
    ft.font = Font(name="Arial", size=8, color="888080", italic=True)
    ft.alignment = Alignment(horizontal="center")

    ws.freeze_panes = "A7"
    ws.auto_filter.ref = f"A6:J{row-3}"
    ws.page_setup.orientation = "landscape"
    ws.page_setup.fitToPage = True; ws.page_setup.fitToWidth = 1
    ws.print_title_rows = "1:6"

    buf = io.BytesIO()
    wb.save(buf); buf.seek(0)
    fname = f"AAE_BOM_{(project or job_num).replace(' ','_')}.xlsx"
    return send_file(buf,
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                     as_attachment=True, download_name=fname)



# ═══════════════════════════════════════════════════════════════════
# ADMIN API ROUTES
# ═══════════════════════════════════════════════════════════════════

# ── Auth: handled client-side (localStorage) ─────────────────────────────────

# ── Labor Rates (admin only) ─────────────────────────────────────────
@app.route("/api/labor_rates", methods=["GET"])
def get_labor_rates_api():
    if not supabase:
        # Return defaults formatted as list
        cats = {
            "enclosure_prep":"Enclosure","subpanel_mount":"Enclosure","panel_layout":"Enclosure",
            "din_rail":"Enclosure","wire_duct":"Enclosure","enc_accessory":"Enclosure","door_component":"Enclosure",
            "main_breaker_small":"Power","main_breaker_large":"Power","branch_breaker_1p":"Power",
            "branch_breaker_23p":"Power","fused_disconnect":"Power","cpt":"Power","pdb":"Power",
            "relay_icecube":"Motor Ctrl","relay_din":"Motor Ctrl","contactor_small":"Motor Ctrl",
            "contactor_large":"Motor Ctrl","overload":"Motor Ctrl","timer":"Motor Ctrl","ssr":"Motor Ctrl",
            "vfd_small":"Motor Ctrl","vfd_med":"Motor Ctrl","vfd_large":"Motor Ctrl",
            "soft_starter_small":"Motor Ctrl","soft_starter_large":"Motor Ctrl",
            "pilot_light":"Control","selector":"Control","pushbutton":"Control","estop":"Control",
            "plc_rack":"PLC/Network","plc_di_do":"PLC/Network","plc_ai_ao":"PLC/Network",
            "hmi":"PLC/Network","safety_relay":"PLC/Network","eth_switch":"PLC/Network","eth_cable":"PLC/Network",
            "tb_standard":"Terminals","tb_ground":"Terminals","tb_fused":"Terminals",
            "tb_disconnect":"Terminals","tb_accessories":"Terminals","terminal_markers":"Terminals",
            "wire_land_control":"Wiring","ferrule":"Wiring","wire_route":"Wiring",
            "heat_shrink_label":"Wiring","heat_shrink_batch":"Wiring",
            "ul_labels":"UL/QC","continuity_check":"UL/QC","hipot":"UL/QC","as_built":"UL/QC","qc_signoff":"UL/QC",
        }
        return jsonify([{"rate_key":k,"rate_value":v,"category":cats.get(k,"Other"),"description":k.replace("_"," ").title()}
                        for k,v in LABOR_RATES.items()])
    try:
        rows = supabase.table("aae_labor_rates").select("*").order("category").order("rate_key").execute()
        return jsonify(rows.data)
    except Exception as e:
        # Table doesn't exist yet or DB error — return hardcoded defaults so UI still works
        print(f"labor_rates DB error (returning defaults): {e}")
        cats = {
            "enclosure_prep":"Enclosure","subpanel_mount":"Enclosure","panel_layout":"Enclosure",
            "din_rail":"Enclosure","wire_duct":"Enclosure","enc_accessory":"Enclosure","door_component":"Enclosure",
            "main_breaker_small":"Power","main_breaker_large":"Power","branch_breaker_1p":"Power",
            "branch_breaker_23p":"Power","fused_disconnect":"Power","cpt":"Power","pdb":"Power",
            "relay_icecube":"Motor Ctrl","relay_din":"Motor Ctrl","contactor_small":"Motor Ctrl",
            "contactor_large":"Motor Ctrl","overload":"Motor Ctrl","timer":"Motor Ctrl","ssr":"Motor Ctrl",
            "vfd_small":"Motor Ctrl","vfd_med":"Motor Ctrl","vfd_large":"Motor Ctrl",
            "soft_starter_small":"Motor Ctrl","soft_starter_large":"Motor Ctrl",
            "pilot_light":"Control","selector":"Control","pushbutton":"Control","estop":"Control",
            "plc_rack":"PLC/Network","plc_di_do":"PLC/Network","plc_ai_ao":"PLC/Network",
            "hmi":"PLC/Network","safety_relay":"PLC/Network","eth_switch":"PLC/Network","eth_cable":"PLC/Network",
            "tb_standard":"Terminals","tb_ground":"Terminals","tb_fused":"Terminals",
            "tb_disconnect":"Terminals","tb_accessories":"Terminals","terminal_markers":"Terminals",
            "wire_land_control":"Wiring","ferrule":"Wiring","wire_route":"Wiring",
            "heat_shrink_label":"Wiring","heat_shrink_batch":"Wiring",
            "ul_labels":"UL/QC","continuity_check":"UL/QC","hipot":"UL/QC","as_built":"UL/QC","qc_signoff":"UL/QC",
        }
        return jsonify([{"rate_key":k,"rate_value":v,"category":cats.get(k,"Other"),"description":k.replace("_"," ").title()}
                        for k,v in LABOR_RATES.items()])

@app.route("/api/labor_rates/<rate_key>", methods=["PUT"])
def update_labor_rate(rate_key):
    data = request.get_json()
    if not supabase: return jsonify({"error":"DB not configured"}), 500
    try:
        updates = {"rate_value":float(data["rate_value"]),"updated_by":data.get("updated_by","mfellers"),"updated_at":datetime.now().isoformat()}
        supabase.table("aae_labor_rates").update(updates).eq("rate_key",rate_key).execute()
        return jsonify({"success":True})
    except Exception as e:
        return jsonify({"error":str(e)}), 500

@app.route("/api/labor_rates/bulk", methods=["POST"])
def bulk_update_labor_rates():
    """Update multiple rates at once."""
    data = request.get_json()
    updates = data.get("rates", {})
    username = data.get("username", "mfellers")
    if not supabase:
        return jsonify({"success": True, "updated": 0, "note": "DB not configured — saved to browser only"})
    try:
        now = datetime.now().isoformat()
        for key, val in updates.items():
            supabase.table("aae_labor_rates").update(
                {"rate_value": float(val), "updated_by": username, "updated_at": now}
            ).eq("rate_key", key).execute()
        return jsonify({"success":True,"updated":len(updates)})
    except Exception as e:
        print(f"bulk_update_labor_rates DB error: {e}")
        return jsonify({"success": True, "updated": 0, "note": f"DB error (table may not exist yet): {e}"})

# ── Vendors ───────────────────────────────────────────────────────────
@app.route("/api/vendors", methods=["GET"])
def get_vendors():
    if not supabase:
        return jsonify([])
    try:
        rows = supabase.table("aae_vendors").select("*").order("vendor_name").order("manufacturer").execute()
        return jsonify(rows.data)
    except Exception as e:
        # Table doesn't exist yet — return empty list so UI uses its built-in defaults
        print(f"vendors DB error (returning empty): {e}")
        return jsonify([])

@app.route("/api/vendors", methods=["POST"])
def create_vendor():
    data = request.get_json()
    if not supabase:
        return jsonify({"success": True, "note": "DB not configured — saved to browser only"})
    try:
        result = supabase.table("aae_vendors").insert({
            "vendor_name":data["vendor_name"],"manufacturer":data["manufacturer"],
            "account_number":data.get("account_number",""),
            "notes":data.get("notes",""),"updated_by":data.get("updated_by","mfellers")
        }).execute()
        return jsonify({"success":True,"vendor":result.data[0]})
    except Exception as e:
        print(f"create_vendor DB error: {e}")
        return jsonify({"success": True, "note": f"Saved to browser only (DB error: {e})"})

@app.route("/api/vendors/<int:vid>", methods=["PUT"])
def update_vendor(vid):
    data = request.get_json()
    if not supabase: return jsonify({"error":"DB not configured"}), 500
    try:
        updates = {k:data[k] for k in ["vendor_name","manufacturer","account_number","notes","active"] if k in data}
        updates["updated_at"] = datetime.now().isoformat()
        updates["updated_by"] = data.get("updated_by","mfellers")
        supabase.table("aae_vendors").update(updates).eq("id",vid).execute()
        return jsonify({"success":True})
    except Exception as e:
        return jsonify({"error":str(e)}), 500

@app.route("/api/vendors/<int:vid>", methods=["DELETE"])
def delete_vendor(vid):
    if not supabase: return jsonify({"error":"DB not configured"}), 500
    try:
        supabase.table("aae_vendors").update({"active":False,"updated_at":datetime.now().isoformat()}).eq("id",vid).execute()
        return jsonify({"success":True})
    except Exception as e:
        return jsonify({"error":str(e)}), 500

# ── Hour Breakdown Report (Excel download) ────────────────────────────
@app.route("/api/hours_report", methods=["POST"])
def hours_report():
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    data      = request.get_json()
    calc      = data.get("calc", {})
    bid       = data.get("bid_data", {})
    breakdown = calc.get("hour_breakdown", {})
    project   = bid.get("project_name","Unknown Project")
    customer  = bid.get("customer_name","")

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Labor Hour Breakdown"
    ws.column_dimensions["A"].width = 36
    ws.column_dimensions["B"].width = 14
    ws.column_dimensions["C"].width = 14
    ws.column_dimensions["D"].width = 40

    RED="9B1B1B"; DARK_RED="6B0A0A"; WHITE="FFFFFF"; LIGHT_RED="FDECEA"
    DARK="2C2C2C"; GRAY="F5F0F0"

    def hdr(row, vals, bg, fg=WHITE, bold=True, sz=10):
        for ci, v in enumerate(vals, 1):
            c = ws.cell(row=row, column=ci, value=v)
            c.font = Font(name="Arial", bold=bold, color=fg, size=sz)
            if bg: c.fill = PatternFill("solid", fgColor=bg)
            c.alignment = Alignment(horizontal="center" if ci>1 else "left", vertical="center", indent=(1 if ci==1 else 0))

    def row_data(r, label, hrs, rate_key=None, qty=None, rate=None, note=""):
        thin = Side(style="thin", color="E0D0D0")
        bdr  = Border(bottom=thin, left=thin, right=thin, top=thin)
        vals = [label, f"{hrs:.2f} hrs" if isinstance(hrs,(int,float)) else hrs,
                f"{rate:.2f} min/ea × {qty}" if (rate and qty is not None) else "", note]
        even = r%2==0
        fill = PatternFill("solid", fgColor=GRAY if even else WHITE)
        for ci, v in enumerate(vals, 1):
            c = ws.cell(row=r, column=ci, value=v)
            c.fill = fill; c.border = bdr
            c.font = Font(name="Arial", size=9, color=DARK)
            c.alignment = Alignment(horizontal="right" if ci==2 else "left", vertical="center")

    # Banner
    ws.merge_cells("A1:D1"); ws.row_dimensions[1].height = 14
    c=ws["A1"]; c.value="AAE AUTOMATION — LABOR HOUR BREAKDOWN REPORT"; c.fill=PatternFill("solid",fgColor=RED)
    c.font=Font(name="Arial",bold=True,color=WHITE,size=11); c.alignment=Alignment(horizontal="center",vertical="center")

    # Project header
    ws.merge_cells("A2:B2"); ws["A2"]=f"Project: {project}"
    ws["A2"].font=Font(name="Arial",bold=True,color=DARK_RED,size=10); ws["A2"].fill=PatternFill("solid",fgColor=LIGHT_RED)
    ws.merge_cells("C2:D2"); ws["C2"]=f"Customer: {customer}"
    ws["C2"].font=Font(name="Arial",size=9,color=DARK); ws["C2"].fill=PatternFill("solid",fgColor=LIGHT_RED)
    ws.row_dimensions[2].height=18

    ws.merge_cells("A3:B3"); ws["A3"]=f"Date: {datetime.now().strftime('%m/%d/%Y %I:%M %p')}"
    ws["A3"].font=Font(name="Arial",size=9,color=DARK); ws["A3"].fill=PatternFill("solid",fgColor=GRAY)
    ws.merge_cells("C3:D3")
    ws["C3"]=f"Complexity: {bid.get('complexity','STANDARD')}  |  Tech: {bid.get('tech_level','JOURNEYMAN')}"
    ws["C3"].font=Font(name="Arial",size=9,color=DARK); ws["C3"].fill=PatternFill("solid",fgColor=GRAY)
    ws.row_dimensions[3].height=15

    # Column headers
    hdr(4, ["SECTION / LINE ITEM","HOURS","RATE × QTY","NOTES"], DARK, WHITE)
    ws.row_dimensions[4].height=18

    r = 5
    def section(title):
        nonlocal r
        ws.merge_cells(f"A{r}:D{r}")
        c=ws.cell(row=r,column=1,value=f"  {title}")
        c.font=Font(name="Arial",bold=True,color=WHITE,size=9)
        c.fill=PatternFill("solid",fgColor=RED)
        c.alignment=Alignment(horizontal="left",vertical="center",indent=1)
        ws.row_dimensions[r].height=16
        r+=1

    def data_row(label, hrs, formula="", note=""):
        nonlocal r
        thin=Side(style="thin",color="E0D0D0"); bdr=Border(bottom=thin,left=thin,right=thin,top=thin)
        fill=PatternFill("solid",fgColor=GRAY if r%2==0 else WHITE)
        for ci,v in enumerate([label, f"{hrs:.2f}" if isinstance(hrs,(int,float)) else hrs, formula, note],1):
            c=ws.cell(row=r,column=ci,value=v)
            c.fill=fill; c.border=bdr
            c.font=Font(name="Arial",size=9,color=DARK)
            c.alignment=Alignment(horizontal="right" if ci==2 else "left",vertical="center")
            if ci==2: c.number_format='0.00'
        ws.row_dimensions[r].height=15
        r+=1

    # Use the exact rates that produced this calculation (from _rates_snapshot)
    # so line-item hours match the section totals perfectly
    rates = calc.get("_rates_snapshot") or get_labor_rates()
    bd    = breakdown

    enc_qty = max(1,int(bid.get("enc_qty",1)))
    din_runs= int(bid.get("din_rail_runs",3)); duct_runs=int(bid.get("wire_duct_runs",4))
    wire_cnt= bd.get("wire_count_used",0)
    tb_std=int(bid.get("tb_standard",0)); tb_gnd=int(bid.get("tb_ground",0))
    tb_fsd=int(bid.get("tb_fused",0));    tb_dis=int(bid.get("tb_disconnect",0))
    tb_total=tb_std+tb_gnd+tb_fsd+tb_dis
    di=int(bid.get("plc_di",0)); do_pts=int(bid.get("plc_do",0))
    ai=int(bid.get("plc_ai",0)); ao=int(bid.get("plc_ao",0))
    relay_ic=int(bid.get("relay_icecube",0))
    plc_yn=str(bid.get("plc_present","N")).upper()=="Y"
    hmi_yn=str(bid.get("hmi_present","N")).upper()=="Y"
    eth_sw=1 if str(bid.get("eth_switch","N")).upper()=="Y" else 0
    eth_cab=int(bid.get("eth_cables",0))
    main_amp=int(bid.get("main_amp",100))
    br_1p=int(bid.get("branch_1p",0)); br_2p=int(bid.get("branch_2p",0)); br_3p=int(bid.get("branch_3p",0))
    fused_d=int(bid.get("fused_disconnects",0))
    estops=int(bid.get("estops",0)); pilots=int(bid.get("pilot_lights",0))
    markers=str(bid.get("terminal_markers","Y")).upper()=="Y"
    ferrules=str(bid.get("ferrules","Y")).upper()=="Y"
    hs_yn=str(bid.get("heat_shrink","Y")).upper()=="Y"

    section("ENCLOSURE & MECHANICAL")
    data_row("Enclosure prep + subpanel mount", (rates["enclosure_prep"]+rates["subpanel_mount"])*enc_qty/60,
             f"({rates['enclosure_prep']}+{rates['subpanel_mount']}) min × {enc_qty} enc")
    data_row("Panel layout", rates["panel_layout"]*enc_qty/60, f"{rates['panel_layout']} min × {enc_qty} enc")
    data_row("DIN rail installation", rates["din_rail"]*din_runs/60, f"{rates['din_rail']} min × {din_runs} runs")
    data_row("Wire duct installation", rates["wire_duct"]*duct_runs*4/60, f"{rates['wire_duct']} min × {duct_runs*4} sections")
    data_row("Enclosure accessories", rates["enc_accessory"]*int(bid.get("enc_accessories",0))/60,
             f"{rates['enc_accessory']} min × {bid.get('enc_accessories',0)} items")
    data_row("► SECTION TOTAL", bd.get("enclosure_hrs",0))

    section("POWER DISTRIBUTION")
    mb_rate = rates["main_breaker_small"] if main_amp<=100 else rates["main_breaker_large"]
    data_row("Main breaker", mb_rate*enc_qty/60, f"{mb_rate} min × {enc_qty}")
    data_row("1-pole branch breakers", rates["branch_breaker_1p"]*br_1p/60, f"{rates['branch_breaker_1p']} min × {br_1p}")
    data_row("2/3-pole branch breakers", rates["branch_breaker_23p"]*(br_2p+br_3p)/60, f"{rates['branch_breaker_23p']} min × {br_2p+br_3p}")
    data_row("Fused disconnects", rates["fused_disconnect"]*fused_d/60, f"{rates['fused_disconnect']} min × {fused_d}")
    data_row("► SECTION TOTAL", bd.get("power_hrs",0))

    section("MOTOR CONTROL")
    vfd_sm  = int(bid.get("vfd_small",0))
    vfd_md  = int(bid.get("vfd_med",0))
    vfd_lg  = int(bid.get("vfd_large",0))
    relay_dn= int(bid.get("relay_din",0))
    cont_sm = int(bid.get("contactor_small",0))
    cont_lg = int(bid.get("contactor_large",0))
    overload= int(bid.get("overload",0))
    timer_q = int(bid.get("timers",0))
    ssr_q   = int(bid.get("ssrs",0))
    ss_sm   = int(bid.get("soft_starter_small",0))
    ss_lg   = int(bid.get("soft_starter_large",0))
    if relay_ic:  data_row(f"Ice cube relays ({relay_ic})",   rates["relay_icecube"]*relay_ic/60,   f"{rates['relay_icecube']} min × {relay_ic}")
    if relay_dn:  data_row(f"DIN relays ({relay_dn})",        rates["relay_din"]*relay_dn/60,       f"{rates['relay_din']} min × {relay_dn}")
    if cont_sm:   data_row(f"Contactors ≤40A ({cont_sm})",    rates["contactor_small"]*cont_sm/60,  f"{rates['contactor_small']} min × {cont_sm}")
    if cont_lg:   data_row(f"Contactors >40A ({cont_lg})",    rates["contactor_large"]*cont_lg/60,  f"{rates['contactor_large']} min × {cont_lg}")
    if overload:  data_row(f"Overload relays ({overload})",   rates["overload"]*overload/60,        f"{rates['overload']} min × {overload}")
    if timer_q:   data_row(f"Timer relays ({timer_q})",       rates["timer"]*timer_q/60,            f"{rates['timer']} min × {timer_q}")
    if ssr_q:     data_row(f"Solid state relays ({ssr_q})",   rates["ssr"]*ssr_q/60,               f"{rates['ssr']} min × {ssr_q}")
    if vfd_sm:    data_row(f"VFD ≤5HP ({vfd_sm})",           rates["vfd_small"]*vfd_sm/60,         f"{rates['vfd_small']} min × {vfd_sm}")
    if vfd_md:    data_row(f"VFD 6–25HP ({vfd_md})",         rates["vfd_med"]*vfd_md/60,           f"{rates['vfd_med']} min × {vfd_md}")
    if vfd_lg:    data_row(f"VFD 26–100HP ({vfd_lg})",       rates["vfd_large"]*vfd_lg/60,         f"{rates['vfd_large']} min × {vfd_lg}")
    if ss_sm:     data_row(f"Soft starters ≤50A ({ss_sm})",  rates["soft_starter_small"]*ss_sm/60, f"{rates['soft_starter_small']} min × {ss_sm}")
    if ss_lg:     data_row(f"Soft starters >50A ({ss_lg})",  rates["soft_starter_large"]*ss_lg/60, f"{rates['soft_starter_large']} min × {ss_lg}")
    if not any([relay_ic,relay_dn,cont_sm,cont_lg,overload,timer_q,ssr_q,vfd_sm,vfd_md,vfd_lg,ss_sm,ss_lg]):
        data_row("(no motor control components)", 0)
    data_row("► SECTION TOTAL", bd.get("motor_ctrl_hrs",0))

    section("CONTROL DEVICES")
    data_row("E-stops", rates["estop"]*estops/60, f"{rates['estop']} min × {estops}")
    data_row("Pilot lights", rates["pilot_light"]*pilots/60, f"{rates['pilot_light']} min × {pilots}")
    data_row("► SECTION TOTAL", bd.get("control_dev_hrs",0))

    section("PLC / NETWORKING")
    data_row("PLC rack/controller", (rates["plc_rack"] if plc_yn else 0)/60, f"{rates['plc_rack']} min (if PLC present)")
    data_row("Digital I/O wiring", rates["plc_di_do"]*(di+do_pts)/60, f"{rates['plc_di_do']} min × {di+do_pts} pts (DI:{di} DO:{do_pts})")
    data_row("Analog I/O wiring", rates["plc_ai_ao"]*(ai+ao)/60, f"{rates['plc_ai_ao']} min × {ai+ao} pts (AI:{ai} AO:{ao})")
    data_row("HMI mount & cable", rates["hmi"]*(1 if hmi_yn else 0)/60, f"{rates['hmi']} min (if HMI present)")
    data_row("Ethernet switch", rates["eth_switch"]*eth_sw/60, f"{rates['eth_switch']} min × {eth_sw}")
    data_row("Ethernet cables", rates["eth_cable"]*eth_cab/60, f"{rates['eth_cable']} min × {eth_cab}")
    data_row("► SECTION TOTAL", bd.get("plc_network_hrs",0))

    section("TERMINAL BLOCKS & WIRING")
    data_row("Standard TBs", rates["tb_standard"]*tb_std/60, f"{rates['tb_standard']} min × {tb_std}")
    data_row("Ground TBs", rates["tb_ground"]*tb_gnd/60, f"{rates['tb_ground']} min × {tb_gnd}")
    data_row("Fused TBs", rates["tb_fused"]*tb_fsd/60, f"{rates['tb_fused']} min × {tb_fsd}")
    data_row(f"Wire landing — {wire_cnt} wires × 2 ends", rates["wire_land_control"]*wire_cnt*2/60,
             f"{rates['wire_land_control']} min × {wire_cnt*2} ends", "AUTO-ESTIMATED wire count" if int(bid.get("wire_count",0))==0 else "")
    data_row(f"Ferrule crimping (if enabled)", (rates["ferrule"]*wire_cnt*2/60) if ferrules else 0,
             f"{rates['ferrule']} min × {wire_cnt*2} ends")
    data_row("Wire routing through duct", rates["wire_route"]*wire_cnt/60, f"{rates['wire_route']} min × {wire_cnt}")
    marker_strips=max(1,int(tb_total*1.2/10)) if markers else 0
    data_row("Terminal marker labeling", rates["terminal_markers"]*marker_strips/60, f"{rates['terminal_markers']} min × {marker_strips} strips")
    data_row("► SECTION TOTAL", bd.get("terminals_wire_hrs",0))

    section("HEAT SHRINK LABELS")
    data_row(f"Label application ({wire_cnt*2} labels)", (rates["heat_shrink_label"]*wire_cnt*2/60) if hs_yn else 0,
             f"{rates['heat_shrink_label']} min × {wire_cnt*2}")
    data_row("Heat shrink batch setups", (rates["heat_shrink_batch"]*max(1,int(wire_cnt*2/50))/60) if hs_yn else 0,
             f"{rates['heat_shrink_batch']} min × {max(1,int(wire_cnt*2/50))} batches")
    data_row("► SECTION TOTAL", bd.get("heat_shrink_hrs",0))

    section("UL LABELING & QC")
    data_row("UL component labeling", rates["ul_labels"]*enc_qty/60, f"{rates['ul_labels']} min × {enc_qty} enc")
    data_row("Continuity check", rates["continuity_check"]*wire_cnt/60, f"{rates['continuity_check']} min × {wire_cnt} wires")
    data_row("Hi-pot test", rates["hipot"]*enc_qty/60, f"{rates['hipot']} min × {enc_qty} enc")
    data_row("As-built drawings", rates["as_built"]*enc_qty/60, f"{rates['as_built']} min × {enc_qty} enc")
    data_row("QC sign-off", rates["qc_signoff"]*enc_qty/60, f"{rates['qc_signoff']} min × {enc_qty} enc")
    data_row("► SECTION TOTAL", bd.get("ul_qc_hrs",0))

    # Summary block
    r += 1
    ws.merge_cells(f"A{r}:D{r}")
    sc = ws.cell(row=r, column=1, value="  SUMMARY")
    sc.fill = PatternFill("solid", fgColor=DARK_RED)
    sc.font = Font(name="Arial", bold=True, color=WHITE, size=10)
    sc.alignment = Alignment(horizontal="left", vertical="center", indent=1)
    ws.row_dimensions[r].height = 18
    r += 1

    fat_hrs  = float(bid.get("fat_hours", 0))
    eng_hrs  = float(bid.get("eng_hours", 0))
    prog_hrs = float(bid.get("prog_hours", 0))
    data_row("RAW HOURS (sum of all sections)", calc.get("raw_hours", 0))
    data_row(f"Complexity multiplier ({bid.get('complexity','STANDARD')})",
             calc.get("raw_hours",0) * (calc.get("complexity_mult",1.0) - 1.0),
             f"x {calc.get('complexity_mult',1.0):.2f}")
    data_row(f"Tech level multiplier ({bid.get('tech_level','JOURNEYMAN')})", 0,
             f"x {calc.get('tech_mult',1.0):.2f}")
    data_row("Calibration factor", 0, f"x {calc.get('calib_factor',1.0):.3f}")
    data_row("FAT / witness testing hours", fat_hrs)
    data_row("Engineering hours", eng_hrs)
    data_row("Programming hours", prog_hrs)

    # Total row — no merge conflicts: A=label, B=empty, C=value, D=note
    r += 1
    ws.merge_cells(f"A{r}:B{r}")
    tc = ws.cell(row=r, column=1, value="  TOTAL BILLABLE HOURS")
    tc.font = Font(name="Arial", bold=True, color=WHITE, size=12)
    tc.fill = PatternFill("solid", fgColor=RED)
    tc.alignment = Alignment(horizontal="left", vertical="center", indent=1)
    tv = ws.cell(row=r, column=3, value=calc.get("total_hours", 0))
    tv.font = Font(name="Arial", bold=True, color=WHITE, size=12)
    tv.fill = PatternFill("solid", fgColor=RED)
    tv.alignment = Alignment(horizontal="center", vertical="center")
    tv.number_format = '0.00'
    td = ws.cell(row=r, column=4,
                 value=f"@ ${calc.get('labor_rate_used',95):.0f}/hr = ${calc.get('labor_cost',0):,.2f}")
    td.font = Font(name="Arial", bold=True, color="F4A9A8", size=10)
    td.fill = PatternFill("solid", fgColor=RED)
    ws.row_dimensions[r].height = 24

    ws.freeze_panes="A5"
    ws.page_setup.orientation="landscape"; ws.page_setup.fitToPage=True; ws.page_setup.fitToWidth=1

    buf=io.BytesIO(); wb.save(buf); buf.seek(0)
    fname=f"AAE_HourBreakdown_{project.replace(' ','_')}.xlsx"
    return send_file(buf,mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                     as_attachment=True,download_name=fname)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
