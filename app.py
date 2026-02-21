import os, json, base64, re
from flask import Flask, render_template, request, jsonify, send_file
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
    "labor_rate": 85.0, "mat_markup": 0.30,
    "overhead": 0.12, "profit": 0.18, "expedite": 0.15,
    "wire_16_per_ft": 0.18, "wire_14_per_ft": 0.22,
    "wire_12_per_ft": 0.28, "wire_10_per_ft": 0.38,
    "heat_shrink_each": 0.08,
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
    wire_cnt = int(data.get("wire_count", 0))
    wire_len = float(data.get("wire_avg_len", 24))
    hs_yn    = str(data.get("heat_shrink","Y")).upper()=="Y"
    fat_hrs  = float(data.get("fat_hours", 0))
    eng_hrs  = float(data.get("eng_hours", 0))
    prog_hrs = float(data.get("prog_hours", 0))
    comp_key = data.get("complexity", "STANDARD")
    tech_key = data.get("tech_level", "JOURNEYMAN")
    expedite = str(data.get("expedite","N")).upper()=="Y"
    lr_override = float(data.get("labor_rate_override", 0))

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

    c_mult  = COMPLEXITY_MULT.get(comp_key, 1.0)
    t_mult  = TECH_MULT.get(tech_key, 1.0)
    total_hrs = raw_hrs * c_mult * t_mult + fat_hrs + eng_hrs + prog_hrs

    # Material / BOM
    ctrl_wire_ft  = int(wire_cnt * wire_len / 12 * 1.15)
    pwr_wire_ft   = int((br_1p + (br_2p+br_3p)*2) * 1.5)
    hs_labels_qty = wire_cnt * 2 if hs_yn else 0
    tb_marker_strips = max(0, int(tb_total*1.2/10)) if markers else 0
    ferrule_bags  = max(0, int(wire_cnt*2/500)+1) if ferrules else 0
    din_sticks    = din_runs
    duct_sections = duct_runs * 2
    eth_cables_qty= eth_cab
    spare_tbs     = int(tb_std * 0.1)

    mat_cost = (
        ctrl_wire_ft * SHOP_RATES["wire_16_per_ft"] +
        pwr_wire_ft  * SHOP_RATES["wire_12_per_ft"] +
        hs_labels_qty* SHOP_RATES["heat_shrink_each"] +
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

    # Pricing
    eff_labor_rate = lr_override if lr_override > 0 else SHOP_RATES["labor_rate"]
    labor_cost = total_hrs * eff_labor_rate
    mat_with_markup = mat_cost * (1 + SHOP_RATES["mat_markup"])
    subtotal = labor_cost + mat_with_markup
    overhead_cost = subtotal * SHOP_RATES["overhead"]
    pre_profit = subtotal + overhead_cost
    exp_cost = pre_profit * SHOP_RATES["expedite"] if expedite else 0
    total_price = pre_profit / (1 - SHOP_RATES["profit"]) + exp_cost

    return {
        "raw_hours": round(raw_hrs, 2),
        "complexity_mult": c_mult,
        "tech_mult": t_mult,
        "total_hours": round(total_hrs, 2),
        "wire_count": wire_cnt,
        "hs_labels_qty": hs_labels_qty,
        "ctrl_wire_ft": ctrl_wire_ft,
        "pwr_wire_ft": pwr_wire_ft,
        "mat_cost_raw": round(mat_cost, 2),
        "mat_cost_markup": round(mat_with_markup, 2),
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
        return {"error": str(e), "quantities": {}, "extraction_summary": {"confidence": 0}}

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

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
