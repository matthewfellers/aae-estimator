import os, json, base64, re
from flask import Flask, render_template, request, jsonify, send_file, send_from_directory
from anthropic import Anthropic
from supabase import create_client
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
anthropic = Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
supabase  = create_client(
    os.environ.get("SUPABASE_URL"),
    os.environ.get("SUPABASE_ANON_KEY")
)

# ── Labor rates (minutes per unit) ────────────────────────────────────────
LABOR_RATES = {
    "enclosure_prep": 30, "subpanel_mount": 20, "din_rail": 10,
    "wire_duct": 8, "enc_accessory": 12, "door_component": 20,
    "panel_layout": 45,
    "main_breaker_small": 25, "main_breaker_large": 45,
    "branch_breaker_1p": 12, "branch_breaker_23p": 18,
    "fused_disconnect": 30, "cpt": 35, "pdb": 25,
    "relay_icecube": 15, "relay_din": 12, "contactor_small": 25,
    "contactor_large": 40, "overload": 20, "timer": 15, "ssr": 18,
    "pilot_light": 10, "selector": 15, "pushbutton": 12, "estop": 18,
    "vfd_small": 60, "vfd_med": 90, "vfd_large": 150,
    "soft_starter_small": 75, "soft_starter_large": 120,
    "plc_rack": 45, "plc_di_do": 8, "plc_ai_ao": 12, "hmi": 40,
    "safety_relay": 35, "eth_switch": 30, "eth_cable": 8,
    "tb_standard": 4, "tb_ground": 5, "tb_fused": 8, "tb_disconnect": 7,
    "tb_accessories": 5, "terminal_markers": 6,
    "wire_land_control": 3, "ferrule": 1.5, "wire_route": 2,
    "heat_shrink_label": 1.5, "heat_shrink_batch": 5,
    "ul_labels": 45, "continuity_check": 1.5, "hipot": 30,
    "as_built": 60, "qc_signoff": 30,
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
def calculate_bid(data):
    r = LABOR_RATES
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
    if wire_cnt == 0:
        wire_cnt = int((di+do_pts)*1.2 + (ai+ao)*1.5 + tb_total*0.8 + br_1p*2 + (br_2p+br_3p)*3)

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
        "tech_mult": t_mult,
        "calib_factor": calib_factor,
        "total_hours": round(total_hrs, 2),
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
    prompt = """You are an expert electrical estimator at AAE Automation, a UL-508A certified 
industrial control panel shop. Analyze this electrical drawing set and extract ALL component 
quantities needed for a panel bid.

Extract and count every component you can identify. Return ONLY valid JSON — no markdown, 
no explanation, no code blocks. Just the raw JSON object.

{
  "extraction_summary": {
    "drawing_types_found": ["BOM", "SCHEMATIC", "TERMINAL_SCHEDULE", "IO_LIST"],
    "confidence": 0.0,
    "scope_gap_flags": [],
    "review_flags": []
  },
  "quantities": {
    "enc_qty": 1,
    "din_rail_runs": 3,
    "wire_duct_runs": 4,
    "enc_accessories": 0,
    "main_amp": 100,
    "main_disconnect_type": "",
    "branch_1p": 0,
    "branch_2p": 0,
    "branch_3p": 0,
    "fused_disconnects": 0,
    "cpt_present": "N",
    "cpt_kva": 0,
    "pdb_qty": 0,
    "relay_icecube": 0,
    "relay_din": 0,
    "contactor_small": 0,
    "contactor_large": 0,
    "overload": 0,
    "timers": 0,
    "ssrs": 0,
    "pilot_lights": 0,
    "selectors": 0,
    "push_buttons": 0,
    "estops": 0,
    "vfd_small": 0,
    "vfd_med": 0,
    "vfd_large": 0,
    "soft_starter_small": 0,
    "soft_starter_large": 0,
    "plc_present": "N",
    "plc_manufacturer": "",
    "plc_model": "",
    "plc_di": 0,
    "plc_do": 0,
    "plc_ai": 0,
    "plc_ao": 0,
    "hmi_present": "N",
    "hmi_size": 0,
    "safety_relay": "N",
    "eth_switch": "N",
    "eth_cables": 0,
    "tb_standard": 0,
    "tb_ground": 0,
    "tb_fused": 0,
    "tb_disconnect": 0,
    "wire_count": 0,
    "wire_avg_len": 24
  },
  "component_list": [
    {"ref_des": "", "description": "", "part_number": "", "qty": 0, "confidence": 0.0}
  ]
}

Rules:
- VFDs: <=5HP = small, 6-25HP = med, 26-100HP = large
- Contactors: <=40A = small, >40A = large  
- Soft starters: <=50A = small, >50A = large
- Count wire numbers if a wire schedule exists for wire_count
- If no wire schedule: estimate wire_count as (DI+DO)*1.2 + (AI+AO)*1.5 + terminals*0.8
- Flag anything uncertain in review_flags
- confidence: 0.0 to 1.0"""

    try:
        response = anthropic.messages.create(
            model="claude-sonnet-4-20250514",
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
        # Strip markdown if model wrapped it anyway
        raw = re.sub(r"^```json\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        return json.loads(raw)
    except Exception as e:
        import traceback
        err_detail = traceback.format_exc()
        print("SCAN ERROR:", err_detail)  # shows in Railway logs
        return {"error": str(e), "error_detail": err_detail,
                "quantities": {}, "extraction_summary": {"confidence": 0}}

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
        resp = anthropic.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=20,
            messages=[{"role":"user","content":"Reply with the word OK only."}]
        )
        return jsonify({"status": "ok", "reply": resp.content[0].text,
                        "anthropic_sdk": anthropic.__version__})
    except Exception as e:
        import traceback
        return jsonify({"status": "error", "error": str(e),
                        "detail": traceback.format_exc()}), 500


@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/scan", methods=["POST"])
def scan():
    if "drawing" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
    f = request.files["drawing"]
    pdf_bytes = f.read()
    pdf_b64   = base64.standard_b64encode(pdf_bytes).decode("utf-8")
    result    = scan_drawing(pdf_b64, f.filename)
    return jsonify(result)

@app.route("/api/calculate", methods=["POST"])
def calculate():
    data = request.get_json()
    result = calculate_bid(data)
    return jsonify(result)

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
        result = supabase.table("bids").insert(row).execute()
        return jsonify({"success": True, "id": result.data[0]["id"]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/bids", methods=["GET"])
def get_bids():
    try:
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
    """Generate a BOM Excel file for download."""
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    data      = request.get_json()
    bid_data  = data.get("bid_data", {})
    calc_data = data.get("calc", {})
    quote_num = data.get("quote_num", f"AAE-{datetime.now().strftime('%Y%m%d')}")

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "BOM"

    red_fill   = PatternFill("solid", fgColor="9B1B1B")
    gray_fill  = PatternFill("solid", fgColor="E8E4E0")
    white_font = Font(bold=True, color="FFFFFF", name="Calibri")
    head_font  = Font(bold=True, name="Calibri")
    mono_font  = Font(name="Courier New", size=10)
    thin       = Side(style="thin", color="CCCCCC")
    border     = Border(bottom=Side(style="thin", color="CCCCCC"))

    # Title rows
    ws.merge_cells("A1:G1")
    ws["A1"] = "AAE AUTOMATION, INC. — BILL OF MATERIALS"
    ws["A1"].font = Font(bold=True, size=14, color="9B1B1B", name="Calibri")
    ws.merge_cells("A2:G2")
    ws["A2"] = f"Customer: {bid_data.get('customer_name','')}   |   Project: {bid_data.get('project_name','')}   |   Date: {datetime.now().strftime('%m/%d/%Y')}   |   Quote #: {quote_num}"
    ws["A2"].font = Font(size=10, name="Calibri")
    ws.merge_cells("A3:G3")
    ws["A3"] = "*** INTERNAL DOCUMENT ONLY — Not for customer distribution ***"
    ws["A3"].font = Font(bold=True, color="CC6600", name="Calibri")
    ws.row_dimensions[3].height = 18

    # Headers row 5
    headers = ["Category", "Item / Part #", "Description", "Qty", "Unit", "Unit Cost ($)", "Total Cost ($)"]
    for ci, h in enumerate(headers, 1):
        cell = ws.cell(row=5, column=ci, value=h)
        cell.fill = red_fill
        cell.font = white_font
        cell.alignment = Alignment(horizontal="center")

    ws.column_dimensions["A"].width = 18
    ws.column_dimensions["B"].width = 20
    ws.column_dimensions["C"].width = 42
    ws.column_dimensions["D"].width = 8
    ws.column_dimensions["E"].width = 8
    ws.column_dimensions["F"].width = 14
    ws.column_dimensions["G"].width = 14

    # Build BOM rows from calc data
    bom_items = build_bom_items(bid_data, calc_data)
    row = 6
    last_cat = None
    for item in bom_items:
        if item["category"] != last_cat:
            last_cat = item["category"]
            ws.merge_cells(f"A{row}:G{row}")
            c = ws.cell(row=row, column=1, value=item["category"].upper())
            c.fill = PatternFill("solid", fgColor="2C2C2C")
            c.font = Font(bold=True, color="FFFFFF", name="Calibri", size=9)
            row += 1
        fill = PatternFill("solid", fgColor="FAFAFA") if row % 2 == 0 else PatternFill("solid", fgColor="FFFFFF")
        vals = [item["category"], item["part_num"], item["description"],
                item["qty"], item["unit"], round(item["unit_cost"], 2), round(item["total_cost"], 2)]
        for ci, v in enumerate(vals, 1):
            c = ws.cell(row=row, column=ci, value=v)
            c.fill = fill
            c.font = mono_font if ci in (4, 6, 7) else Font(name="Calibri", size=10)
            if ci in (6, 7):
                c.number_format = '"$"#,##0.00'
                c.alignment = Alignment(horizontal="right")
        row += 1

    # Totals
    total_raw = sum(i["total_cost"] for i in bom_items)
    ws.cell(row=row+1, column=5, value="TOTAL RAW COST:").font = Font(bold=True, name="Calibri")
    tc = ws.cell(row=row+1, column=7, value=round(total_raw, 2))
    tc.font = Font(bold=True, color="9B1B1B", name="Calibri")
    tc.number_format = '"$"#,##0.00'

    # Labor summary sheet
    ws2 = wb.create_sheet("Labor Summary")
    labor_rows = [
        ("Raw Labor Hours", f"{calc_data.get('raw_hours', 0):.2f} hrs"),
        ("Complexity Multiplier", f"{calc_data.get('complexity_mult', 1.0)}×"),
        ("Tech Multiplier", f"{calc_data.get('tech_mult', 1.0)}×"),
        ("Calibration Factor", f"{calc_data.get('calib_factor', 1.0):.3f}×"),
        ("Total Billable Hours", f"{calc_data.get('total_hours', 0):.2f} hrs"),
        ("Labor Rate", f"${calc_data.get('labor_rate_used', 0):.2f}/hr"),
        ("Labor Cost", calc_data.get("labor_cost", 0)),
        ("Material (raw)", calc_data.get("mat_cost_raw", 0)),
        ("Wire Cost", calc_data.get("wire_cost", 0)),
        ("Heat Shrink Cost", calc_data.get("hs_cost", 0)),
        ("Material w/ Margin", calc_data.get("mat_cost_markup", 0)),
        ("Overhead (12%)", calc_data.get("overhead_cost", 0)),
        ("Expedite", calc_data.get("expedite_cost", 0)),
        ("TOTAL QUOTED PRICE", calc_data.get("total_price", 0)),
    ]
    ws2.column_dimensions["A"].width = 26
    ws2.column_dimensions["B"].width = 18
    for ri, (label, val) in enumerate(labor_rows, 1):
        lc = ws2.cell(row=ri, column=1, value=label)
        vc = ws2.cell(row=ri, column=2, value=val)
        lc.font = Font(bold=(ri == len(labor_rows)), name="Calibri")
        if isinstance(val, float):
            vc.number_format = '"$"#,##0.00'
            vc.font = Font(bold=(ri == len(labor_rows)),
                           color=("9B1B1B" if ri == len(labor_rows) else "000000"), name="Calibri")
        if ri == len(labor_rows):
            lc.font = Font(bold=True, color="9B1B1B", name="Calibri")

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return send_file(buf,
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                     as_attachment=True,
                     download_name=f"AAE_BOM_{quote_num}.xlsx")


@app.route("/api/bid_quote_pdf/<int:bid_id>", methods=["GET"])
def bid_quote_pdf(bid_id):
    """Download quote PDF for a saved bid by ID."""
    try:
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
    def add(cat, part, desc, qty, unit, unit_cost):
        if qty <= 0: return
        items.append({"category": cat, "part_num": part, "description": desc,
                       "qty": qty, "unit": unit, "unit_cost": unit_cost,
                       "total_cost": qty * unit_cost})

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
    add("Terminals", "TB-STD", "Standard Terminal Block", int(bid_data.get("tb_standard", 0)), "ea", 3.50)
    add("Terminals", "TB-GND", "Ground Terminal Block", int(bid_data.get("tb_ground", 0)), "ea", 4.20)
    add("Terminals", "TB-FUSED", "Fused Terminal Block", int(bid_data.get("tb_fused", 0)), "ea", 8.50)
    add("Terminals", "TB-DISC", "Disconnect Terminal Block", int(bid_data.get("tb_disconnect", 0)), "ea", 9.80)
    # Wire
    ctrl_ft = calc_data.get("ctrl_wire_ft", 0)
    pwr_ft  = calc_data.get("pwr_wire_ft", 0)
    wrate   = calc_data.get("wire_cost_per_ft", 0.40)
    if ctrl_ft + pwr_ft > 0:
        add("Wiring", "WIRE", f"Control/Power Wire ({ctrl_ft+pwr_ft} ft total)", ctrl_ft + pwr_ft, "ft", wrate)
    # Heat shrink labels
    hs_rolls = calc_data.get("hs_rolls", 0)
    if hs_rolls > 0:
        add("Wiring", "H075X044H1T", f"Heat Shrink Labels — {calc_data.get('hs_labels_qty',0)} pcs ({hs_rolls} roll{'s' if hs_rolls>1 else ''})", hs_rolls, "roll", 275)
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


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
