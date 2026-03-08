-- ============================================================================
-- AAE ERP — Shipping Kit Packages Migration
-- Run in Supabase SQL Editor after shipping_docs_migration_v1.0.sql
-- ============================================================================

-- ── 1. Add version + template_group columns to shipping_templates ───────────

ALTER TABLE public.shipping_templates
  ADD COLUMN IF NOT EXISTS version         int  NOT NULL DEFAULT 1,
  ADD COLUMN IF NOT EXISTS template_group  text;

-- Back-fill template_group from template_name for existing rows
UPDATE public.shipping_templates
SET template_group = template_name
WHERE template_group IS NULL;

CREATE INDEX IF NOT EXISTS idx_shipping_templates_group
  ON public.shipping_templates (org_id, template_group, version DESC);

-- ── 2. shipping_kit_configs — saved kit configurations ──────────────────────

CREATE TABLE IF NOT EXISTS public.shipping_kit_configs (
  id            bigserial   PRIMARY KEY,
  org_id        uuid        NOT NULL DEFAULT public.current_org_id()
                            REFERENCES public.orgs(id) ON DELETE SET NULL,
  created_by    uuid        DEFAULT auth.uid()
                            REFERENCES auth.users(id) ON DELETE SET NULL,
  config_name   text        NOT NULL,
  description   text,
  options       jsonb       NOT NULL DEFAULT '[]',
  is_deleted    boolean     NOT NULL DEFAULT false,
  created_at    timestamptz NOT NULL DEFAULT now(),
  updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_shipping_kit_configs_org
  ON public.shipping_kit_configs (org_id);

-- ── 3. shipping_kit_doc_rules — documents to generate per kit ───────────────

CREATE TABLE IF NOT EXISTS public.shipping_kit_doc_rules (
  id                bigserial   PRIMARY KEY,
  org_id            uuid        NOT NULL DEFAULT public.current_org_id()
                                REFERENCES public.orgs(id) ON DELETE SET NULL,
  kit_config_id     bigint      NOT NULL
                                REFERENCES public.shipping_kit_configs(id) ON DELETE CASCADE,
  doc_type          text        NOT NULL DEFAULT 'sticker',
  template_id       bigint      REFERENCES public.shipping_templates(id) ON DELETE SET NULL,
  template_group    text,
  template_version  int,
  packing_tmpl_id   bigint      REFERENCES public.packing_templates(id) ON DELETE SET NULL,
  doc_label         text        NOT NULL,
  copies_per_sn     int         NOT NULL DEFAULT 1,
  per_page          int         NOT NULL DEFAULT 1,
  condition         text        NOT NULL DEFAULT 'always',
  folder_path       text        NOT NULL DEFAULT '',
  sort_order        int         NOT NULL DEFAULT 0,
  created_at        timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT chk_doc_type CHECK (doc_type IN ('sticker', 'packing_slip')),
  CONSTRAINT chk_per_page CHECK (per_page IN (1, 2)),
  CONSTRAINT chk_condition CHECK (condition IN ('always', 'rio_520', 'stateline'))
);

CREATE INDEX IF NOT EXISTS idx_shipping_kit_doc_rules_config
  ON public.shipping_kit_doc_rules (kit_config_id, sort_order);
CREATE INDEX IF NOT EXISTS idx_shipping_kit_doc_rules_org
  ON public.shipping_kit_doc_rules (org_id);

-- ── 4. shipping_bulk_jobs — history of generated packages ───────────────────

CREATE TABLE IF NOT EXISTS public.shipping_bulk_jobs (
  id                bigserial   PRIMARY KEY,
  org_id            uuid        NOT NULL DEFAULT public.current_org_id()
                                REFERENCES public.orgs(id) ON DELETE SET NULL,
  created_by        uuid        DEFAULT auth.uid()
                                REFERENCES auth.users(id) ON DELETE SET NULL,
  kit_config_id     bigint      REFERENCES public.shipping_kit_configs(id) ON DELETE SET NULL,
  config_name       text        NOT NULL,
  serial_numbers    jsonb       NOT NULL DEFAULT '[]',
  field_values      jsonb       NOT NULL DEFAULT '{}',
  options_selected  jsonb       NOT NULL DEFAULT '{}',
  doc_count         int         NOT NULL DEFAULT 0,
  is_deleted        boolean     NOT NULL DEFAULT false,
  generated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_shipping_bulk_jobs_org
  ON public.shipping_bulk_jobs (org_id, generated_at DESC);

-- ============================================================================
-- RLS — org-scoped SELECT for all, admin INSERT/UPDATE/DELETE for configs/rules
-- ============================================================================

-- ── shipping_kit_configs RLS ────────────────────────────────────────────────

ALTER TABLE public.shipping_kit_configs ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.shipping_kit_configs FORCE ROW LEVEL SECURITY;

CREATE POLICY "skc_select_org"
ON public.shipping_kit_configs FOR SELECT TO authenticated
USING (
  org_id = public.current_org_id()
  AND is_deleted = false
);

CREATE POLICY "skc_insert_admin"
ON public.shipping_kit_configs FOR INSERT TO authenticated
WITH CHECK (
  org_id = public.current_org_id()
  AND public.is_admin()
);

CREATE POLICY "skc_update_admin"
ON public.shipping_kit_configs FOR UPDATE TO authenticated
USING (
  public.is_admin()
  AND org_id = public.current_org_id()
)
WITH CHECK (
  public.is_admin()
  AND org_id = public.current_org_id()
);

CREATE POLICY "skc_delete_admin"
ON public.shipping_kit_configs FOR DELETE TO authenticated
USING (
  public.is_admin()
  AND org_id = public.current_org_id()
);

-- ── shipping_kit_doc_rules RLS ──────────────────────────────────────────────

ALTER TABLE public.shipping_kit_doc_rules ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.shipping_kit_doc_rules FORCE ROW LEVEL SECURITY;

CREATE POLICY "skdr_select_org"
ON public.shipping_kit_doc_rules FOR SELECT TO authenticated
USING (
  org_id = public.current_org_id()
);

CREATE POLICY "skdr_insert_admin"
ON public.shipping_kit_doc_rules FOR INSERT TO authenticated
WITH CHECK (
  org_id = public.current_org_id()
  AND public.is_admin()
);

CREATE POLICY "skdr_update_admin"
ON public.shipping_kit_doc_rules FOR UPDATE TO authenticated
USING (
  public.is_admin()
  AND org_id = public.current_org_id()
)
WITH CHECK (
  public.is_admin()
  AND org_id = public.current_org_id()
);

CREATE POLICY "skdr_delete_admin"
ON public.shipping_kit_doc_rules FOR DELETE TO authenticated
USING (
  public.is_admin()
  AND org_id = public.current_org_id()
);

-- ── shipping_bulk_jobs RLS ──────────────────────────────────────────────────

ALTER TABLE public.shipping_bulk_jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.shipping_bulk_jobs FORCE ROW LEVEL SECURITY;

CREATE POLICY "sbj_select_creator"
ON public.shipping_bulk_jobs FOR SELECT TO authenticated
USING (
  created_by = auth.uid()
  AND org_id = public.current_org_id()
  AND is_deleted = false
);

CREATE POLICY "sbj_select_privileged"
ON public.shipping_bulk_jobs FOR SELECT TO authenticated
USING (
  (public.is_admin() OR public.is_accounting())
  AND org_id = public.current_org_id()
  AND is_deleted = false
);

CREATE POLICY "sbj_insert_self"
ON public.shipping_bulk_jobs FOR INSERT TO authenticated
WITH CHECK (
  org_id = public.current_org_id()
  AND created_by = auth.uid()
);

CREATE POLICY "sbj_update_admin"
ON public.shipping_bulk_jobs FOR UPDATE TO authenticated
USING (
  public.is_admin()
  AND org_id = public.current_org_id()
)
WITH CHECK (
  public.is_admin()
  AND org_id = public.current_org_id()
);

CREATE POLICY "sbj_delete_admin"
ON public.shipping_bulk_jobs FOR DELETE TO authenticated
USING (
  public.is_admin()
  AND org_id = public.current_org_id()
);

-- ── Grant table access to authenticated role ────────────────────────────────

GRANT SELECT, INSERT, UPDATE, DELETE ON public.shipping_kit_configs   TO authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.shipping_kit_doc_rules TO authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.shipping_bulk_jobs     TO authenticated;

GRANT USAGE, SELECT ON SEQUENCE public.shipping_kit_configs_id_seq   TO authenticated;
GRANT USAGE, SELECT ON SEQUENCE public.shipping_kit_doc_rules_id_seq TO authenticated;
GRANT USAGE, SELECT ON SEQUENCE public.shipping_bulk_jobs_id_seq     TO authenticated;

-- ============================================================================
-- SECURITY DEFINER RPC — soft delete functions (admin-only)
-- ============================================================================

CREATE OR REPLACE FUNCTION public.soft_delete_kit_config(p_config_id bigint)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  IF NOT public.is_admin() THEN
    RAISE EXCEPTION 'Admin access required';
  END IF;
  UPDATE public.shipping_kit_configs
  SET is_deleted = true, updated_at = now()
  WHERE id = p_config_id
    AND org_id = public.current_org_id();
END;
$$;

CREATE OR REPLACE FUNCTION public.soft_delete_bulk_job(p_job_id bigint)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  IF NOT public.is_admin() THEN
    RAISE EXCEPTION 'Admin access required';
  END IF;
  UPDATE public.shipping_bulk_jobs
  SET is_deleted = true
  WHERE id = p_job_id
    AND org_id = public.current_org_id();
END;
$$;

GRANT EXECUTE ON FUNCTION public.soft_delete_kit_config(bigint) TO authenticated;
GRANT EXECUTE ON FUNCTION public.soft_delete_bulk_job(bigint)   TO authenticated;

-- ============================================================================
-- Done — Shipping Kit Packages Migration
-- ============================================================================
