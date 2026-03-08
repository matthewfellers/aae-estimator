-- ============================================================================
-- AAE ERP — QuickBooks Scheduling Integration Migration v1.0
-- ============================================================================
-- Run in Supabase SQL Editor (logged in as postgres / service_role).
-- Safe to re-run — uses IF NOT EXISTS / DROP POLICY IF EXISTS throughout.
--
-- Creates:
--   8 tables:  qb_sales_orders, qb_sales_order_lines, qb_item_receipts,
--              qb_item_receipt_lines, qb_items, qb_sync_log,
--              scheduling_part_overrides, rack_panel_mappings
--   RLS policies for all 8 tables
--   Helper views for job readiness calculations
--
-- Data flow:
--   QB Desktop → Polling Service (pywin32 COM) → Supabase (this schema)
--   The ERP reads from these tables. The polling service writes via
--   service-role key. The ERP NEVER has direct QB access.
-- ============================================================================


-- ────────────────────────────────────────────────────────────────────────────
-- 1. TABLES
-- ────────────────────────────────────────────────────────────────────────────

-- 1a. QB_SALES_ORDERS — Sales order headers synced from QuickBooks
CREATE TABLE IF NOT EXISTS public.qb_sales_orders (
  id                uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  org_id            uuid        NOT NULL REFERENCES public.orgs(id) ON DELETE CASCADE,
  qb_txn_id         text        NOT NULL,          -- QB unique transaction ID
  ref_number        text,                           -- SO number (e.g., "SO-1234")
  customer_name     text        NOT NULL,
  txn_date          date,                           -- order date
  ship_date         date,                           -- required ship/completion date
  due_date          date,                           -- payment due date
  po_number         text,                           -- customer PO
  status            text        NOT NULL DEFAULT 'open'
                                CHECK (status IN ('open','closed','cancelled')),
  total_amount      numeric(12,2),
  memo              text,
  is_fully_invoiced boolean     NOT NULL DEFAULT false,
  synced_at         timestamptz NOT NULL DEFAULT now(),
  raw_data          jsonb,                          -- full QB response for debugging
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  UNIQUE (org_id, qb_txn_id)
);

-- 1b. QB_SALES_ORDER_LINES — Line items within sales orders
CREATE TABLE IF NOT EXISTS public.qb_sales_order_lines (
  id                uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  org_id            uuid        NOT NULL REFERENCES public.orgs(id) ON DELETE CASCADE,
  sales_order_id    uuid        NOT NULL REFERENCES public.qb_sales_orders(id) ON DELETE CASCADE,
  qb_txn_line_id    text,                           -- QB line item ID
  line_number       integer,
  item_ref          text,                           -- QB item FullName (part number)
  description       text,
  quantity          numeric(12,4),
  qty_invoiced      numeric(12,4)  DEFAULT 0,       -- how much has shipped/invoiced
  rate              numeric(12,4),
  amount            numeric(12,2),
  is_rack           boolean     NOT NULL DEFAULT false,  -- flag for rack items needing panel breakdown
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  UNIQUE (org_id, sales_order_id, qb_txn_line_id)
);

-- 1c. QB_ITEM_RECEIPTS — Received goods headers from QuickBooks
CREATE TABLE IF NOT EXISTS public.qb_item_receipts (
  id                uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  org_id            uuid        NOT NULL REFERENCES public.orgs(id) ON DELETE CASCADE,
  qb_txn_id         text        NOT NULL,          -- QB unique transaction ID
  vendor_name       text,
  txn_date          date,
  ref_number        text,                           -- vendor invoice/receipt number
  total_amount      numeric(12,2),
  memo              text,
  linked_po         text,                           -- linked purchase order ref
  synced_at         timestamptz NOT NULL DEFAULT now(),
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  UNIQUE (org_id, qb_txn_id)
);

-- 1d. QB_ITEM_RECEIPT_LINES — Individual received items
CREATE TABLE IF NOT EXISTS public.qb_item_receipt_lines (
  id                uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  org_id            uuid        NOT NULL REFERENCES public.orgs(id) ON DELETE CASCADE,
  item_receipt_id   uuid        NOT NULL REFERENCES public.qb_item_receipts(id) ON DELETE CASCADE,
  item_ref          text,                           -- part number (matches sales_order_lines.item_ref)
  description       text,
  quantity          numeric(12,4),
  cost              numeric(12,4),
  amount            numeric(12,2),
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now()
);

-- 1e. QB_ITEMS — Master parts/inventory list from QuickBooks
CREATE TABLE IF NOT EXISTS public.qb_items (
  id                uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  org_id            uuid        NOT NULL REFERENCES public.orgs(id) ON DELETE CASCADE,
  qb_list_id        text        NOT NULL,          -- QB unique list ID
  item_type         text,                           -- Inventory, NonInventory, Service, Assembly, Group
  full_name         text        NOT NULL,          -- fully qualified name (e.g., "Panels:PNL-VFD-001")
  name              text,                           -- short name
  description       text,
  qty_on_hand       numeric(12,4),
  reorder_point     numeric(12,4),
  purchase_cost     numeric(12,4),
  sales_price       numeric(12,4),                 -- for future estimator pricing
  preferred_vendor  text,
  is_active         boolean     NOT NULL DEFAULT true,
  manufacturer      text,                           -- extracted from part prefix
  synced_at         timestamptz NOT NULL DEFAULT now(),
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  UNIQUE (org_id, qb_list_id)
);

-- 1f. QB_SYNC_LOG — Audit trail for polling service runs
CREATE TABLE IF NOT EXISTS public.qb_sync_log (
  id                uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  org_id            uuid        NOT NULL REFERENCES public.orgs(id) ON DELETE CASCADE,
  sync_type         text        NOT NULL           -- 'incremental' or 'full'
                                CHECK (sync_type IN ('incremental','full')),
  started_at        timestamptz NOT NULL,
  completed_at      timestamptz,
  status            text        NOT NULL DEFAULT 'running'
                                CHECK (status IN ('running','success','error')),
  records_synced    jsonb       DEFAULT '{}',       -- {"sales_orders": 15, "item_receipts": 8, ...}
  error_message     text,
  company_file      text,                           -- which QB file was synced
  created_at        timestamptz NOT NULL DEFAULT now()
);

-- 1g. SCHEDULING_PART_OVERRIDES — Manual part availability markings
CREATE TABLE IF NOT EXISTS public.scheduling_part_overrides (
  id                uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  org_id            uuid        NOT NULL REFERENCES public.orgs(id) ON DELETE CASCADE,
  sales_order_id    uuid        REFERENCES public.qb_sales_orders(id) ON DELETE CASCADE,
  item_ref          text        NOT NULL,           -- part number
  override_type     text        NOT NULL            -- 'available' (stocked), 'not_needed', 'substitute'
                                CHECK (override_type IN ('available','not_needed','substitute')),
  substitute_ref    text,                           -- if type=substitute, what part replaces it
  notes             text,
  created_by        uuid        NOT NULL DEFAULT auth.uid(),
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  UNIQUE (org_id, sales_order_id, item_ref)
);

-- 1h. RACK_PANEL_MAPPINGS — Which racks break down into which panels
CREATE TABLE IF NOT EXISTS public.rack_panel_mappings (
  id                uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  org_id            uuid        NOT NULL REFERENCES public.orgs(id) ON DELETE CASCADE,
  rack_item_ref     text        NOT NULL,           -- rack part number in QB (e.g., "RACK-1000")
  panel_name        text        NOT NULL,           -- panel identifier (e.g., "PNL-VFD-001")
  panel_type        text        DEFAULT 'Custom'    -- PLC, MCC, VFD, Relay, Junction Box, Control, Custom
                                CHECK (panel_type IN ('PLC','MCC','VFD','Relay','Junction Box','Control','Custom')),
  estimated_hours   numeric(8,2),
  sort_order        integer     NOT NULL DEFAULT 0,
  notes             text,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  UNIQUE (org_id, rack_item_ref, panel_name)
);

-- 1i. SCHEDULING_JOB_REQUESTS — Manually flagged jobs for scheduling attention
CREATE TABLE IF NOT EXISTS public.scheduling_job_requests (
  id                uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  org_id            uuid        NOT NULL REFERENCES public.orgs(id) ON DELETE CASCADE,
  sales_order_id    uuid        REFERENCES public.qb_sales_orders(id) ON DELETE CASCADE,
  job_id            uuid        REFERENCES public.jobs(id) ON DELETE CASCADE,
  reason            text,                           -- why this was manually flagged
  priority          integer     NOT NULL DEFAULT 3 CHECK (priority BETWEEN 1 AND 5),
  requested_by      uuid        NOT NULL DEFAULT auth.uid(),
  status            text        NOT NULL DEFAULT 'pending'
                                CHECK (status IN ('pending','scheduled','dismissed')),
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now()
);


-- ────────────────────────────────────────────────────────────────────────────
-- 2. INDEXES
-- ────────────────────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_qb_so_org_ship ON public.qb_sales_orders(org_id, ship_date);
CREATE INDEX IF NOT EXISTS idx_qb_so_org_status ON public.qb_sales_orders(org_id, status);
CREATE INDEX IF NOT EXISTS idx_qb_so_lines_order ON public.qb_sales_order_lines(sales_order_id);
CREATE INDEX IF NOT EXISTS idx_qb_so_lines_item ON public.qb_sales_order_lines(org_id, item_ref);
CREATE INDEX IF NOT EXISTS idx_qb_ir_org ON public.qb_item_receipts(org_id, txn_date);
CREATE INDEX IF NOT EXISTS idx_qb_ir_lines_receipt ON public.qb_item_receipt_lines(item_receipt_id);
CREATE INDEX IF NOT EXISTS idx_qb_ir_lines_item ON public.qb_item_receipt_lines(org_id, item_ref);
CREATE INDEX IF NOT EXISTS idx_qb_items_org ON public.qb_items(org_id, full_name);
CREATE INDEX IF NOT EXISTS idx_qb_items_type ON public.qb_items(org_id, item_type);
CREATE INDEX IF NOT EXISTS idx_qb_sync_org ON public.qb_sync_log(org_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_part_overrides_so ON public.scheduling_part_overrides(org_id, sales_order_id);
CREATE INDEX IF NOT EXISTS idx_rack_mappings_rack ON public.rack_panel_mappings(org_id, rack_item_ref);
CREATE INDEX IF NOT EXISTS idx_job_requests_org ON public.scheduling_job_requests(org_id, status);


-- ────────────────────────────────────────────────────────────────────────────
-- 3. ENABLE ROW LEVEL SECURITY
-- ────────────────────────────────────────────────────────────────────────────

ALTER TABLE public.qb_sales_orders          ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.qb_sales_orders          FORCE ROW LEVEL SECURITY;
ALTER TABLE public.qb_sales_order_lines     ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.qb_sales_order_lines     FORCE ROW LEVEL SECURITY;
ALTER TABLE public.qb_item_receipts         ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.qb_item_receipts         FORCE ROW LEVEL SECURITY;
ALTER TABLE public.qb_item_receipt_lines    ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.qb_item_receipt_lines    FORCE ROW LEVEL SECURITY;
ALTER TABLE public.qb_items                 ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.qb_items                 FORCE ROW LEVEL SECURITY;
ALTER TABLE public.qb_sync_log              ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.qb_sync_log              FORCE ROW LEVEL SECURITY;
ALTER TABLE public.scheduling_part_overrides ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.scheduling_part_overrides FORCE ROW LEVEL SECURITY;
ALTER TABLE public.rack_panel_mappings      ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.rack_panel_mappings      FORCE ROW LEVEL SECURITY;
ALTER TABLE public.scheduling_job_requests  ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.scheduling_job_requests  FORCE ROW LEVEL SECURITY;


-- ────────────────────────────────────────────────────────────────────────────
-- 4. RLS POLICIES
-- ────────────────────────────────────────────────────────────────────────────
-- QB staging tables: readable by all authenticated users in the org.
-- Writable ONLY by service-role key (polling service) — no user can write.
-- scheduling_part_overrides & rack_panel_mappings: writable by scheduler roles.
-- qb_sync_log: readable by admin/supervisor, writable by service-role only.

-- ── 4a. QB_SALES_ORDERS ──────────────────────────────────────────────────

DROP POLICY IF EXISTS "qb_so_select_org" ON public.qb_sales_orders;
CREATE POLICY "qb_so_select_org" ON public.qb_sales_orders
  FOR SELECT TO authenticated
  USING (org_id = public.current_org_id());

-- No INSERT/UPDATE/DELETE policies for authenticated users.
-- Only service-role key (polling service) can write. This is enforced by
-- having FORCE ROW LEVEL SECURITY with no write policies for 'authenticated'.

-- ── 4b. QB_SALES_ORDER_LINES ─────────────────────────────────────────────

DROP POLICY IF EXISTS "qb_sol_select_org" ON public.qb_sales_order_lines;
CREATE POLICY "qb_sol_select_org" ON public.qb_sales_order_lines
  FOR SELECT TO authenticated
  USING (org_id = public.current_org_id());

-- ── 4c. QB_ITEM_RECEIPTS ─────────────────────────────────────────────────

DROP POLICY IF EXISTS "qb_ir_select_org" ON public.qb_item_receipts;
CREATE POLICY "qb_ir_select_org" ON public.qb_item_receipts
  FOR SELECT TO authenticated
  USING (org_id = public.current_org_id());

-- ── 4d. QB_ITEM_RECEIPT_LINES ────────────────────────────────────────────

DROP POLICY IF EXISTS "qb_irl_select_org" ON public.qb_item_receipt_lines;
CREATE POLICY "qb_irl_select_org" ON public.qb_item_receipt_lines
  FOR SELECT TO authenticated
  USING (org_id = public.current_org_id());

-- ── 4e. QB_ITEMS ─────────────────────────────────────────────────────────

DROP POLICY IF EXISTS "qb_items_select_org" ON public.qb_items;
CREATE POLICY "qb_items_select_org" ON public.qb_items
  FOR SELECT TO authenticated
  USING (org_id = public.current_org_id());

-- ── 4f. QB_SYNC_LOG ──────────────────────────────────────────────────────
-- Only admin/supervisor can view sync status

DROP POLICY IF EXISTS "qb_sync_select_admin" ON public.qb_sync_log;
CREATE POLICY "qb_sync_select_admin" ON public.qb_sync_log
  FOR SELECT TO authenticated
  USING (
    org_id = public.current_org_id()
    AND (public.is_admin() OR public.is_supervisor())
  );

-- ── 4g. SCHEDULING_PART_OVERRIDES ────────────────────────────────────────
-- Readable by all org users. Writable by scheduler roles (admin/supervisor)
-- and manufacturing.

DROP POLICY IF EXISTS "part_overrides_select_org" ON public.scheduling_part_overrides;
CREATE POLICY "part_overrides_select_org" ON public.scheduling_part_overrides
  FOR SELECT TO authenticated
  USING (org_id = public.current_org_id());

DROP POLICY IF EXISTS "part_overrides_insert_scheduler" ON public.scheduling_part_overrides;
CREATE POLICY "part_overrides_insert_scheduler" ON public.scheduling_part_overrides
  FOR INSERT TO authenticated
  WITH CHECK (
    org_id = public.current_org_id()
    AND (public.is_scheduler() OR public.is_manufacturing())
  );

DROP POLICY IF EXISTS "part_overrides_update_scheduler" ON public.scheduling_part_overrides;
CREATE POLICY "part_overrides_update_scheduler" ON public.scheduling_part_overrides
  FOR UPDATE TO authenticated
  USING (
    org_id = public.current_org_id()
    AND (public.is_scheduler() OR public.is_manufacturing())
  )
  WITH CHECK (
    org_id = public.current_org_id()
    AND (public.is_scheduler() OR public.is_manufacturing())
  );

DROP POLICY IF EXISTS "part_overrides_delete_scheduler" ON public.scheduling_part_overrides;
CREATE POLICY "part_overrides_delete_scheduler" ON public.scheduling_part_overrides
  FOR DELETE TO authenticated
  USING (
    org_id = public.current_org_id()
    AND (public.is_scheduler() OR public.is_manufacturing())
  );

-- ── 4h. RACK_PANEL_MAPPINGS ──────────────────────────────────────────────
-- Readable by all org users. Writable by scheduler roles only.

DROP POLICY IF EXISTS "rack_mappings_select_org" ON public.rack_panel_mappings;
CREATE POLICY "rack_mappings_select_org" ON public.rack_panel_mappings
  FOR SELECT TO authenticated
  USING (org_id = public.current_org_id());

DROP POLICY IF EXISTS "rack_mappings_insert_scheduler" ON public.rack_panel_mappings;
CREATE POLICY "rack_mappings_insert_scheduler" ON public.rack_panel_mappings
  FOR INSERT TO authenticated
  WITH CHECK (public.is_scheduler() AND org_id = public.current_org_id());

DROP POLICY IF EXISTS "rack_mappings_update_scheduler" ON public.rack_panel_mappings;
CREATE POLICY "rack_mappings_update_scheduler" ON public.rack_panel_mappings
  FOR UPDATE TO authenticated
  USING  (public.is_scheduler() AND org_id = public.current_org_id())
  WITH CHECK (public.is_scheduler() AND org_id = public.current_org_id());

DROP POLICY IF EXISTS "rack_mappings_delete_scheduler" ON public.rack_panel_mappings;
CREATE POLICY "rack_mappings_delete_scheduler" ON public.rack_panel_mappings
  FOR DELETE TO authenticated
  USING (public.is_scheduler() AND org_id = public.current_org_id());

-- ── 4i. SCHEDULING_JOB_REQUESTS ──────────────────────────────────────────
-- Readable by all org users. Writable by scheduler + manufacturing roles.

DROP POLICY IF EXISTS "job_requests_select_org" ON public.scheduling_job_requests;
CREATE POLICY "job_requests_select_org" ON public.scheduling_job_requests
  FOR SELECT TO authenticated
  USING (org_id = public.current_org_id());

DROP POLICY IF EXISTS "job_requests_insert" ON public.scheduling_job_requests;
CREATE POLICY "job_requests_insert" ON public.scheduling_job_requests
  FOR INSERT TO authenticated
  WITH CHECK (
    org_id = public.current_org_id()
    AND (public.is_scheduler() OR public.is_manufacturing())
  );

DROP POLICY IF EXISTS "job_requests_update" ON public.scheduling_job_requests;
CREATE POLICY "job_requests_update" ON public.scheduling_job_requests
  FOR UPDATE TO authenticated
  USING (
    org_id = public.current_org_id()
    AND (public.is_scheduler() OR public.is_manufacturing())
  )
  WITH CHECK (
    org_id = public.current_org_id()
    AND (public.is_scheduler() OR public.is_manufacturing())
  );

DROP POLICY IF EXISTS "job_requests_delete" ON public.scheduling_job_requests;
CREATE POLICY "job_requests_delete" ON public.scheduling_job_requests
  FOR DELETE TO authenticated
  USING (org_id = public.current_org_id() AND public.is_admin());


-- ────────────────────────────────────────────────────────────────────────────
-- 5. HELPER VIEWS
-- ────────────────────────────────────────────────────────────────────────────

-- 5a. Job readiness view — shows parts status for each open sales order
CREATE OR REPLACE VIEW public.v_job_readiness AS
SELECT
  so.id                     AS sales_order_id,
  so.org_id,
  so.ref_number,
  so.customer_name,
  so.ship_date,
  so.status                 AS so_status,
  so.total_amount,
  so.po_number,
  COUNT(sol.id)             AS total_parts,
  COUNT(CASE
    WHEN irl.id IS NOT NULL              THEN 1  -- received via item receipt
    WHEN po.override_type = 'available'  THEN 1  -- manually marked available
    WHEN po.override_type = 'not_needed' THEN 1  -- not needed
    WHEN qi.qty_on_hand > 0              THEN 1  -- in stock per QB
    ELSE NULL
  END)                      AS parts_ready,
  ROUND(
    CASE WHEN COUNT(sol.id) > 0
      THEN COUNT(CASE
        WHEN irl.id IS NOT NULL              THEN 1
        WHEN po.override_type = 'available'  THEN 1
        WHEN po.override_type = 'not_needed' THEN 1
        WHEN qi.qty_on_hand > 0              THEN 1
        ELSE NULL
      END)::numeric / COUNT(sol.id) * 100
      ELSE 0
    END, 1
  )                         AS readiness_pct,
  CASE
    WHEN COUNT(sol.id) = 0 THEN 'no_parts'
    WHEN COUNT(CASE
      WHEN irl.id IS NOT NULL OR po.override_type IN ('available','not_needed') OR qi.qty_on_hand > 0
      THEN 1 ELSE NULL END)::numeric / NULLIF(COUNT(sol.id), 0) >= 0.8
    THEN 'ready'
    WHEN COUNT(CASE
      WHEN irl.id IS NOT NULL OR po.override_type IN ('available','not_needed') OR qi.qty_on_hand > 0
      THEN 1 ELSE NULL END)::numeric / NULLIF(COUNT(sol.id), 0) >= 0.5
    THEN 'partial'
    ELSE 'waiting'
  END                       AS readiness_status
FROM public.qb_sales_orders so
LEFT JOIN public.qb_sales_order_lines sol ON sol.sales_order_id = so.id
-- Check if part was received
LEFT JOIN LATERAL (
  SELECT irl2.id
  FROM public.qb_item_receipt_lines irl2
  WHERE irl2.org_id = so.org_id
    AND irl2.item_ref = sol.item_ref
  LIMIT 1
) irl ON true
-- Check for manual override
LEFT JOIN public.scheduling_part_overrides po
  ON po.org_id = so.org_id
  AND po.sales_order_id = so.id
  AND po.item_ref = sol.item_ref
-- Check QB inventory
LEFT JOIN public.qb_items qi
  ON qi.org_id = so.org_id
  AND qi.full_name = sol.item_ref
  AND qi.is_active = true
WHERE so.status = 'open'
GROUP BY so.id, so.org_id, so.ref_number, so.customer_name,
         so.ship_date, so.status, so.total_amount, so.po_number;

-- 5b. Latest sync status view
CREATE OR REPLACE VIEW public.v_qb_sync_status AS
SELECT DISTINCT ON (org_id)
  org_id,
  sync_type,
  started_at,
  completed_at,
  status,
  records_synced,
  error_message,
  company_file
FROM public.qb_sync_log
ORDER BY org_id, started_at DESC;


-- ────────────────────────────────────────────────────────────────────────────
-- 6. GRANT ACCESS TO VIEWS
-- ────────────────────────────────────────────────────────────────────────────

GRANT SELECT ON public.v_job_readiness TO authenticated;
GRANT SELECT ON public.v_qb_sync_status TO authenticated;


-- ────────────────────────────────────────────────────────────────────────────
-- 7. updated_at TRIGGER FUNCTION (reuse if exists, create if not)
-- ────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION public.set_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

-- Apply to tables with updated_at column
DO $$
DECLARE
  tbl text;
BEGIN
  FOR tbl IN SELECT unnest(ARRAY[
    'qb_sales_orders', 'qb_sales_order_lines',
    'qb_item_receipts', 'qb_item_receipt_lines',
    'qb_items', 'scheduling_part_overrides',
    'rack_panel_mappings', 'scheduling_job_requests'
  ])
  LOOP
    EXECUTE format(
      'DROP TRIGGER IF EXISTS trg_updated_at ON public.%I; '
      'CREATE TRIGGER trg_updated_at BEFORE UPDATE ON public.%I '
      'FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();',
      tbl, tbl
    );
  END LOOP;
END;
$$;


-- ============================================================================
-- MIGRATION COMPLETE
-- ============================================================================
-- Next steps:
--   1. Run this SQL in Supabase SQL Editor
--   2. Deploy the QB polling service on the QB server machine
--   3. Configure config.json with your Supabase URL + service-role key
--   4. Restore your QB backup file and run the first sync
-- ============================================================================
