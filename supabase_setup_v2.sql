-- ════════════════════════════════════════════════════════════════════
-- AAE Automation Estimator — Supabase Schema v2
-- Run this in Supabase SQL Editor to add new tables
-- ════════════════════════════════════════════════════════════════════

-- ── Users table (replaces hardcoded JS users) ───────────────────────
CREATE TABLE IF NOT EXISTS aae_users (
    id          uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    username    text UNIQUE NOT NULL,
    password    text NOT NULL,
    display_name text NOT NULL,
    role        text DEFAULT 'estimator',  -- 'admin' or 'estimator'
    active      boolean DEFAULT true,
    created_at  timestamptz DEFAULT now(),
    updated_at  timestamptz DEFAULT now()
);

-- Seed default users
INSERT INTO aae_users (username, password, display_name, role) VALUES
    ('mfellers',  'aae2025',  'M. Fellers',  'admin'),
    ('admin',     'aae2025',  'Admin',        'admin'),
    ('estimator', 'panel123', 'Estimator',    'estimator')
ON CONFLICT (username) DO NOTHING;

-- ── Labor rates table (editable by admin) ───────────────────────────
CREATE TABLE IF NOT EXISTS aae_labor_rates (
    id          serial PRIMARY KEY,
    rate_key    text UNIQUE NOT NULL,
    rate_value  numeric NOT NULL,
    category    text NOT NULL,
    description text NOT NULL,
    unit        text DEFAULT 'min/unit',
    updated_by  text,
    updated_at  timestamptz DEFAULT now()
);

-- Seed with current calibrated rates
INSERT INTO aae_labor_rates (rate_key, rate_value, category, description, unit) VALUES
    -- Enclosure & Mechanical
    ('enclosure_prep',      20,   'Enclosure', 'Enclosure prep & layout',              'min/enclosure'),
    ('subpanel_mount',      15,   'Enclosure', 'Sub-panel mounting',                   'min/enclosure'),
    ('panel_layout',        25,   'Enclosure', 'Panel layout planning',                'min/enclosure'),
    ('din_rail',             6,   'Enclosure', 'DIN rail installation',                'min/run'),
    ('wire_duct',            4,   'Enclosure', 'Wire duct installation',               'min/duct section'),
    ('enc_accessory',        8,   'Enclosure', 'Enclosure accessory (fan, light...)',  'min/item'),
    ('door_component',      12,   'Enclosure', 'Door-mounted component',               'min/item'),
    -- Power Distribution
    ('main_breaker_small',  15,   'Power', 'Main breaker ≤100A',                       'min/breaker'),
    ('main_breaker_large',  30,   'Power', 'Main breaker >100A',                       'min/breaker'),
    ('branch_breaker_1p',    6,   'Power', '1-pole branch breaker',                    'min/breaker'),
    ('branch_breaker_23p',  10,   'Power', '2 or 3-pole branch breaker',               'min/breaker'),
    ('fused_disconnect',    15,   'Power', 'Fused disconnect',                         'min/unit'),
    ('cpt',                 20,   'Power', 'Control power transformer',                'min/unit'),
    ('pdb',                 12,   'Power', 'Power distribution block',                 'min/unit'),
    -- Motor Control
    ('relay_icecube',        8,   'Motor Ctrl', 'Ice cube relay',                      'min/relay'),
    ('relay_din',            6,   'Motor Ctrl', 'DIN mount relay',                     'min/relay'),
    ('contactor_small',     15,   'Motor Ctrl', 'Contactor ≤40A',                      'min/contactor'),
    ('contactor_large',     25,   'Motor Ctrl', 'Contactor >40A',                      'min/contactor'),
    ('overload',            10,   'Motor Ctrl', 'Overload relay',                      'min/unit'),
    ('timer',                8,   'Motor Ctrl', 'Timer relay',                         'min/unit'),
    ('ssr',                 10,   'Motor Ctrl', 'Solid state relay',                   'min/unit'),
    ('vfd_small',           40,   'Motor Ctrl', 'VFD ≤5HP',                            'min/VFD'),
    ('vfd_med',             65,   'Motor Ctrl', 'VFD 6-25HP',                          'min/VFD'),
    ('vfd_large',          100,   'Motor Ctrl', 'VFD 26-100HP',                        'min/VFD'),
    ('soft_starter_small',  50,   'Motor Ctrl', 'Soft starter ≤50A',                   'min/unit'),
    ('soft_starter_large',  80,   'Motor Ctrl', 'Soft starter >50A',                   'min/unit'),
    -- Control Devices
    ('pilot_light',          6,   'Control', 'Pilot light',                            'min/item'),
    ('selector',             8,   'Control', 'Selector switch',                        'min/item'),
    ('pushbutton',           6,   'Control', 'Push button',                            'min/item'),
    ('estop',               12,   'Control', 'E-stop button',                          'min/item'),
    -- PLC / Networking
    ('plc_rack',            35,   'PLC/Network', 'PLC rack/controller mounting',        'min/rack'),
    ('plc_di_do',          2.5,   'PLC/Network', 'Digital I/O point wiring',            'min/point'),
    ('plc_ai_ao',          3.5,   'PLC/Network', 'Analog I/O point wiring',             'min/point'),
    ('hmi',                 30,   'PLC/Network', 'HMI mounting & cabling',              'min/HMI'),
    ('safety_relay',        20,   'PLC/Network', 'Safety relay',                        'min/unit'),
    ('eth_switch',          15,   'PLC/Network', 'Ethernet switch',                     'min/switch'),
    ('eth_cable',            5,   'PLC/Network', 'Ethernet cable routing & landing',    'min/cable'),
    -- Terminal Blocks
    ('tb_standard',        2.5,   'Terminals', 'Standard terminal block',               'min/TB'),
    ('tb_ground',          2.5,   'Terminals', 'Ground terminal block',                 'min/TB'),
    ('tb_fused',             4,   'Terminals', 'Fused terminal block',                  'min/TB'),
    ('tb_disconnect',        4,   'Terminals', 'Disconnect terminal block',             'min/TB'),
    ('tb_accessories',       3,   'Terminals', 'TB accessories (end caps, markers...)', 'min/set'),
    ('terminal_markers',     3,   'Terminals', 'Terminal marker strip labeling',         'min/strip'),
    -- Wiring
    ('wire_land_control', 1.2,   'Wiring', 'Control wire landing (both ends)',          'min/wire'),
    ('ferrule',           0.25,   'Wiring', 'Ferrule crimping (both ends)',              'min/wire'),
    ('wire_route',        0.35,   'Wiring', 'Wire routing through duct',                'min/wire'),
    ('heat_shrink_label', 0.25,   'Wiring', 'Heat shrink label application',            'min/label'),
    ('heat_shrink_batch',    2,   'Wiring', 'Heat shrink batch setup',                  'min/50 labels'),
    -- UL / QC
    ('ul_labels',           15,   'UL/QC', 'UL component labeling',                    'min/enclosure'),
    ('continuity_check',   0.4,   'UL/QC', 'Continuity check per wire',                'min/wire'),
    ('hipot',               12,   'UL/QC', 'Hi-pot test',                              'min/enclosure'),
    ('as_built',            25,   'UL/QC', 'As-built drawing update',                  'min/enclosure'),
    ('qc_signoff',          12,   'UL/QC', 'QC sign-off',                              'min/enclosure')
ON CONFLICT (rate_key) DO UPDATE SET
    rate_value = EXCLUDED.rate_value,
    updated_at = now();

-- ── Vendor / Manufacturer mapping table ─────────────────────────────
CREATE TABLE IF NOT EXISTS aae_vendors (
    id              serial PRIMARY KEY,
    vendor_name     text NOT NULL,
    manufacturer    text UNIQUE NOT NULL,
    account_number  text,
    contact_name    text,
    phone           text,
    notes           text,
    active          boolean DEFAULT true,
    updated_by      text,
    updated_at      timestamptz DEFAULT now()
);

-- Seed vendor / manufacturer mappings (from user specification)
INSERT INTO aae_vendors (vendor_name, manufacturer) VALUES
    ('Rexel',     'Allen Bradley'),
    ('Rexel',     'Rockwell Automation'),
    ('Rexel',     'Hammond Power'),
    ('Rexel',     'Panduit'),
    ('Rexel',     'Mersen'),
    ('Rexel',     'Hoffman'),
    ('Rexel',     'N-Tron'),
    ('Rexel',     'Corning'),
    ('AWC',       'Phoenix Contact'),
    ('AWC',       'Rittal'),
    ('AWC',       'Hammond Enclosures'),
    ('AWC',       'Siemens'),
    ('AWC',       'Solar Shield'),
    ('AWC',       'Bussmann'),
    ('AWC',       'Marathon Special Products'),
    ('AWC',       'Tripp Lite'),
    ('A-Tech',    'Turck'),
    ('A-Tech',    'Red Lion'),
    ('Graybar',   'Square D'),
    ('Graybar',   'Schneider Electric'),
    ('TD Synnex', 'Cisco'),
    ('Saginaw',   'Saginaw Control Engineering'),
    ('Saginaw',   'SCE')
ON CONFLICT (manufacturer) DO NOTHING;

-- ── RLS: allow anon key full access (adjust for prod) ───────────────
ALTER TABLE aae_users       ENABLE ROW LEVEL SECURITY;
ALTER TABLE aae_labor_rates ENABLE ROW LEVEL SECURITY;
ALTER TABLE aae_vendors     ENABLE ROW LEVEL SECURITY;

CREATE POLICY "allow_all_aae_users"       ON aae_users       FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "allow_all_aae_labor"       ON aae_labor_rates FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "allow_all_aae_vendors"     ON aae_vendors     FOR ALL USING (true) WITH CHECK (true);
