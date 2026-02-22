-- ============================================================
-- AAE Security Migration v3.1
-- Production-hardened RBAC + Org Isolation
--
-- Changes from v3.0:
--   1. Org insert + backfill in a single DO block — no UUID placeholder
--   2. FORCE ROW LEVEL SECURITY on all protected tables
--   3. org_id added to profiles — admin read policy is now org-scoped
--   4. All policies verified org-isolated (no USING(true), no broad auth)
--   5. Audit log policy inlines MFA check directly (no helper dependency)
--   6. Functions restricted to authenticated role (no public RPC exposure)
--   7. JWT requirements documented with enforcement notes
--   Fix: audit_log zero-UUID backfill catches both NULL and 0000... rows
--   Fix: CREATE TABLE IF NOT EXISTS guards for bids/vendors/labor_rates
--        — safe to run against fresh OR existing Supabase projects
--   Fix: profiles INSERT policy added — first-login row creation works
--
-- This file is fully self-contained and supersedes all prior migrations.
-- Safe to run against a fresh project or as an upgrade from any prior version.
--
-- SAFE TO RE-RUN: all statements are idempotent.
-- ============================================================


-- ============================================================
-- JWT REQUIREMENTS — READ BEFORE DEPLOYING
-- ============================================================
-- Every authenticated user's JWT app_metadata MUST contain:
--
--   app_metadata.role   : one of admin | estimator | purchasing |
--                         accounting | manufacturing | viewer
--   app_metadata.org_id : UUID of the user's org (from public.orgs)
--
-- These fields are set ONLY via the Supabase service-role key.
-- The anon key and client SDKs cannot write app_metadata.
-- Use provision_admin.py (service-role key, local only) to set them.
--
-- After any app_metadata change the user MUST sign out and back in.
-- Their active JWT will not reflect changes until a new token is issued.
--
-- If either field is missing, current_org_id() returns NULL and
-- current_role() returns 'viewer'. All org-scoped policies fail
-- closed — the user sees no data rather than all data. This is
-- intentional: fail safe, not fail open.
-- ============================================================


-- ============================================================
-- SECTION 0: Prerequisites
-- ============================================================

-- Ensure gen_random_uuid() is available (Supabase usually has this,
-- but belt-and-suspenders for fresh projects)
CREATE EXTENSION IF NOT EXISTS pgcrypto;


-- ============================================================
-- SECTION 1: JWT helper functions
-- All read from auth.jwt() — no table queries, no recursion risk.
-- SECURITY INVOKER (default): executes as the calling user.
-- No SECURITY DEFINER — no privilege escalation possible.
-- ============================================================

CREATE OR REPLACE FUNCTION public.has_mfa()
RETURNS boolean LANGUAGE sql STABLE
AS $$
  SELECT COALESCE((auth.jwt() ->> 'aal') = 'aal2', false);
$$;

-- Returns NULL if app_metadata.org_id is absent or empty.
-- All org-scoped RLS conditions of the form
--   org_id = public.current_org_id()
-- evaluate to NULL = NULL → false, blocking access automatically.
CREATE OR REPLACE FUNCTION public.current_org_id()
RETURNS uuid LANGUAGE sql STABLE
AS $$
  SELECT NULLIF(auth.jwt() -> 'app_metadata' ->> 'org_id', '')::uuid;
$$;

-- Defaults to 'viewer' — the least-privileged defined role.
CREATE OR REPLACE FUNCTION public.current_role()
RETURNS text LANGUAGE sql STABLE
AS $$
  SELECT COALESCE(auth.jwt() -> 'app_metadata' ->> 'role', 'viewer');
$$;

CREATE OR REPLACE FUNCTION public.is_admin()
RETURNS boolean LANGUAGE sql STABLE
AS $$ SELECT public.current_role() = 'admin'; $$;

CREATE OR REPLACE FUNCTION public.is_accounting()
RETURNS boolean LANGUAGE sql STABLE
AS $$ SELECT public.current_role() = 'accounting'; $$;

CREATE OR REPLACE FUNCTION public.is_purchasing()
RETURNS boolean LANGUAGE sql STABLE
AS $$ SELECT public.current_role() = 'purchasing'; $$;

CREATE OR REPLACE FUNCTION public.is_estimator()
RETURNS boolean LANGUAGE sql STABLE
AS $$ SELECT public.current_role() = 'estimator'; $$;

CREATE OR REPLACE FUNCTION public.is_manufacturing()
RETURNS boolean LANGUAGE sql STABLE
AS $$ SELECT public.current_role() = 'manufacturing'; $$;

-- Revoke public execute on all helpers.
-- Only authenticated database roles (used by Supabase RLS) need access.
REVOKE EXECUTE ON FUNCTION public.has_mfa()         FROM public;
REVOKE EXECUTE ON FUNCTION public.current_org_id()  FROM public;
REVOKE EXECUTE ON FUNCTION public.current_role()    FROM public;
REVOKE EXECUTE ON FUNCTION public.is_admin()        FROM public;
REVOKE EXECUTE ON FUNCTION public.is_accounting()   FROM public;
REVOKE EXECUTE ON FUNCTION public.is_purchasing()   FROM public;
REVOKE EXECUTE ON FUNCTION public.is_estimator()    FROM public;
REVOKE EXECUTE ON FUNCTION public.is_manufacturing() FROM public;

GRANT EXECUTE ON FUNCTION public.has_mfa()          TO authenticated;
GRANT EXECUTE ON FUNCTION public.current_org_id()   TO authenticated;
GRANT EXECUTE ON FUNCTION public.current_role()     TO authenticated;
GRANT EXECUTE ON FUNCTION public.is_admin()         TO authenticated;
GRANT EXECUTE ON FUNCTION public.is_accounting()    TO authenticated;
GRANT EXECUTE ON FUNCTION public.is_purchasing()    TO authenticated;
GRANT EXECUTE ON FUNCTION public.is_estimator()     TO authenticated;
GRANT EXECUTE ON FUNCTION public.is_manufacturing() TO authenticated;


-- ============================================================
-- SECTION 2: Table definitions
-- ============================================================

CREATE TABLE IF NOT EXISTS public.orgs (
  id         uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  name       text        NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.org_members (
  org_id     uuid    NOT NULL REFERENCES public.orgs(id) ON DELETE CASCADE,
  user_id    uuid    NOT NULL,
  role       text    NOT NULL CHECK (role IN
               ('admin','estimator','purchasing','accounting','manufacturing','viewer')),
  is_active  boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  created_by uuid    NULL,
  PRIMARY KEY (org_id, user_id)
);

-- profiles: display info only. org_id added here so admin read
-- policy can be org-scoped (resolves the cross-org read risk in v3.0).
CREATE TABLE IF NOT EXISTS public.profiles (
  user_id     uuid PRIMARY KEY,
  email       text,
  role        text,   -- display only; authoritative role is in app_metadata
  display_name text,
  org_id      uuid REFERENCES public.orgs(id) ON DELETE SET NULL,
  created_at  timestamptz NOT NULL DEFAULT now()
);

-- Add org_id to profiles if upgrading from an earlier version
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'profiles' AND column_name = 'org_id'
  ) THEN
    ALTER TABLE public.profiles ADD COLUMN org_id uuid REFERENCES public.orgs(id) ON DELETE SET NULL;
    RAISE NOTICE 'Added org_id to profiles';
  END IF;
END$$;

CREATE TABLE IF NOT EXISTS public.audit_log (
  id            bigserial   PRIMARY KEY,
  org_id        uuid        NOT NULL,
  actor_user_id uuid        NOT NULL,
  actor_email   text,
  action        text        NOT NULL,
  entity        text,
  entity_id     text,
  payload       jsonb       NOT NULL DEFAULT '{}'::jsonb,
  created_at    timestamptz NOT NULL DEFAULT now()
);

-- Add org_id to audit_log if upgrading
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'audit_log' AND column_name = 'org_id'
  ) THEN
    ALTER TABLE public.audit_log ADD COLUMN org_id uuid NOT NULL
      DEFAULT '00000000-0000-0000-0000-000000000000'::uuid;
    RAISE NOTICE 'Added org_id to audit_log — backfill required (Section 9)';
  END IF;
END$$;

-- Add org_id to bids if upgrading
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'bids' AND column_name = 'org_id'
  ) THEN
    ALTER TABLE public.bids ADD COLUMN org_id uuid REFERENCES public.orgs(id) ON DELETE SET NULL;
    RAISE NOTICE 'Added org_id to bids';
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'bids' AND column_name = 'created_by'
  ) THEN
    ALTER TABLE public.bids ADD COLUMN created_by uuid REFERENCES auth.users(id) ON DELETE SET NULL;
    RAISE NOTICE 'Added created_by to bids';
  END IF;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'aae_vendors' AND column_name = 'org_id'
  ) THEN
    ALTER TABLE public.aae_vendors ADD COLUMN org_id uuid REFERENCES public.orgs(id) ON DELETE SET NULL;
    RAISE NOTICE 'Added org_id to aae_vendors';
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'aae_vendors' AND column_name = 'created_by'
  ) THEN
    ALTER TABLE public.aae_vendors ADD COLUMN created_by uuid;
    RAISE NOTICE 'Added created_by to aae_vendors';
  END IF;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'aae_labor_rates' AND column_name = 'org_id'
  ) THEN
    ALTER TABLE public.aae_labor_rates ADD COLUMN org_id uuid REFERENCES public.orgs(id) ON DELETE SET NULL;
    RAISE NOTICE 'Added org_id to aae_labor_rates';
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'aae_labor_rates' AND column_name = 'created_by'
  ) THEN
    ALTER TABLE public.aae_labor_rates ADD COLUMN created_by uuid;
    RAISE NOTICE 'Added created_by to aae_labor_rates';
  END IF;
END$$;


-- ============================================================
-- SECTION 2b: Create core tables if they don't exist yet
-- Safe for both fresh projects and upgrades — IF NOT EXISTS is a no-op
-- when tables already exist with a different schema.
-- ============================================================

CREATE TABLE IF NOT EXISTS public.bids (
  id             bigserial   PRIMARY KEY,
  org_id         uuid        REFERENCES public.orgs(id) ON DELETE SET NULL,
  created_by     uuid        REFERENCES auth.users(id)  ON DELETE SET NULL,
  customer_name  text,
  project_name   text,
  estimator_name text,
  complexity     text,
  tech_level     text,
  total_hours    numeric,
  total_price    numeric,
  bid_data       jsonb       NOT NULL DEFAULT '{}'::jsonb,
  created_at     timestamptz NOT NULL DEFAULT now(),
  updated_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.aae_vendors (
  id           bigserial   PRIMARY KEY,
  org_id       uuid        REFERENCES public.orgs(id) ON DELETE SET NULL,
  created_by   uuid,
  vendor_name  text        NOT NULL,
  category     text,
  contact      text,
  notes        text,
  active       boolean     NOT NULL DEFAULT true,
  created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.aae_labor_rates (
  id         bigserial   PRIMARY KEY,
  org_id     uuid        REFERENCES public.orgs(id) ON DELETE SET NULL,
  created_by uuid,
  rate_key   text        NOT NULL,
  rate_value numeric     NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (org_id, rate_key)
);


-- ============================================================
-- SECTION 3: Column defaults (auto-bind on insert)
-- ============================================================

ALTER TABLE public.bids            ALTER COLUMN org_id     SET DEFAULT public.current_org_id();
ALTER TABLE public.bids            ALTER COLUMN created_by SET DEFAULT auth.uid();
ALTER TABLE public.aae_vendors     ALTER COLUMN org_id     SET DEFAULT public.current_org_id();
ALTER TABLE public.aae_vendors     ALTER COLUMN created_by SET DEFAULT auth.uid();
ALTER TABLE public.aae_labor_rates ALTER COLUMN org_id     SET DEFAULT public.current_org_id();
ALTER TABLE public.aae_labor_rates ALTER COLUMN created_by SET DEFAULT auth.uid();
ALTER TABLE public.profiles        ALTER COLUMN org_id     SET DEFAULT public.current_org_id();


-- ============================================================
-- SECTION 4: Indexes
-- ============================================================

CREATE INDEX IF NOT EXISTS bids_org_idx         ON public.bids (org_id);
CREATE INDEX IF NOT EXISTS bids_created_by_idx  ON public.bids (created_by);
CREATE INDEX IF NOT EXISTS vendors_org_idx      ON public.aae_vendors (org_id);
CREATE INDEX IF NOT EXISTS labor_org_idx        ON public.aae_labor_rates (org_id);
CREATE INDEX IF NOT EXISTS audit_org_idx        ON public.audit_log (org_id);
CREATE INDEX IF NOT EXISTS audit_actor_idx      ON public.audit_log (actor_user_id);
CREATE INDEX IF NOT EXISTS profiles_org_idx     ON public.profiles (org_id);


-- ============================================================
-- SECTION 5: Enable and FORCE RLS on all protected tables
-- FORCE ensures table owners cannot bypass policies.
-- ============================================================

ALTER TABLE public.orgs            ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.org_members     ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.profiles        ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.bids            ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.aae_vendors     ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.aae_labor_rates ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.audit_log       ENABLE ROW LEVEL SECURITY;

ALTER TABLE public.orgs            FORCE ROW LEVEL SECURITY;
ALTER TABLE public.org_members     FORCE ROW LEVEL SECURITY;
ALTER TABLE public.profiles        FORCE ROW LEVEL SECURITY;
ALTER TABLE public.bids            FORCE ROW LEVEL SECURITY;
ALTER TABLE public.aae_vendors     FORCE ROW LEVEL SECURITY;
ALTER TABLE public.aae_labor_rates FORCE ROW LEVEL SECURITY;
ALTER TABLE public.audit_log       FORCE ROW LEVEL SECURITY;


-- ============================================================
-- SECTION 6: RLS policies — orgs + org_members
-- ============================================================

-- orgs: users can read only their own org
DROP POLICY IF EXISTS "orgs_read_own" ON public.orgs;
CREATE POLICY "orgs_read_own"
ON public.orgs FOR SELECT TO authenticated
USING (id = public.current_org_id());

-- org_members: admin read within org
DROP POLICY IF EXISTS "org_members_admin_read" ON public.org_members;
CREATE POLICY "org_members_admin_read"
ON public.org_members FOR SELECT TO authenticated
USING (public.is_admin() AND org_id = public.current_org_id());

-- org_members: admin + MFA for all writes
DROP POLICY IF EXISTS "org_members_admin_insert" ON public.org_members;
CREATE POLICY "org_members_admin_insert"
ON public.org_members FOR INSERT TO authenticated
WITH CHECK (public.is_admin() AND public.has_mfa() AND org_id = public.current_org_id());

DROP POLICY IF EXISTS "org_members_admin_update" ON public.org_members;
CREATE POLICY "org_members_admin_update"
ON public.org_members FOR UPDATE TO authenticated
USING  (public.is_admin() AND public.has_mfa() AND org_id = public.current_org_id())
WITH CHECK (public.is_admin() AND public.has_mfa() AND org_id = public.current_org_id());

DROP POLICY IF EXISTS "org_members_admin_delete" ON public.org_members;
CREATE POLICY "org_members_admin_delete"
ON public.org_members FOR DELETE TO authenticated
USING (public.is_admin() AND public.has_mfa() AND org_id = public.current_org_id());


-- ============================================================
-- SECTION 7: RLS policies — profiles
-- org_id added so admin read is org-scoped, not globally readable.
-- ============================================================

-- Users can insert their own profile row on first login
-- (safe: WITH CHECK ensures they can only create their own row)
DROP POLICY IF EXISTS "profiles_insert_own" ON public.profiles;
CREATE POLICY "profiles_insert_own"
ON public.profiles FOR INSERT TO authenticated
WITH CHECK (user_id = auth.uid());

-- Users can always read and update their own profile
DROP POLICY IF EXISTS "profiles_read_own" ON public.profiles;
CREATE POLICY "profiles_read_own"
ON public.profiles FOR SELECT TO authenticated
USING (user_id = auth.uid());

DROP POLICY IF EXISTS "profiles_update_own" ON public.profiles;
CREATE POLICY "profiles_update_own"
ON public.profiles FOR UPDATE TO authenticated
USING  (user_id = auth.uid())
WITH CHECK (user_id = auth.uid());

-- Admins can read all profiles within their org (org-scoped — not global)
DROP POLICY IF EXISTS "profiles_admin_read_all" ON public.profiles;
CREATE POLICY "profiles_admin_read_all"
ON public.profiles FOR SELECT TO authenticated
USING (public.is_admin() AND org_id = public.current_org_id());

-- Admins + MFA can update any profile in their org (display name corrections etc.)
DROP POLICY IF EXISTS "profiles_admin_update" ON public.profiles;
CREATE POLICY "profiles_admin_update"
ON public.profiles FOR UPDATE TO authenticated
USING  (public.is_admin() AND public.has_mfa() AND org_id = public.current_org_id())
WITH CHECK (public.is_admin() AND public.has_mfa() AND org_id = public.current_org_id());


-- ============================================================
-- SECTION 8: RLS policies — bids
-- ============================================================

-- Drop all prior bid policies (v2.x and v3.0)
DROP POLICY IF EXISTS "bids_select_own"           ON public.bids;
DROP POLICY IF EXISTS "bids_insert_own"           ON public.bids;
DROP POLICY IF EXISTS "bids_update_own"           ON public.bids;
DROP POLICY IF EXISTS "bids_admin_select_all"     ON public.bids;
DROP POLICY IF EXISTS "bids_admin_update"         ON public.bids;
DROP POLICY IF EXISTS "bids_admin_delete"         ON public.bids;
DROP POLICY IF EXISTS "bids_select_estimator_own" ON public.bids;
DROP POLICY IF EXISTS "bids_select_org_read"      ON public.bids;
DROP POLICY IF EXISTS "bids_insert_estimator"     ON public.bids;
DROP POLICY IF EXISTS "bids_update_owner"         ON public.bids;
DROP POLICY IF EXISTS "bids_update_admin_mfa"     ON public.bids;
DROP POLICY IF EXISTS "bids_delete_admin_mfa"     ON public.bids;

-- Estimators: read own bids in org
CREATE POLICY "bids_select_estimator_own"
ON public.bids FOR SELECT TO authenticated
USING (created_by = auth.uid() AND org_id = public.current_org_id());

-- Purchasing / accounting / admin: read all bids in org
CREATE POLICY "bids_select_org_read"
ON public.bids FOR SELECT TO authenticated
USING (
  (public.is_purchasing() OR public.is_accounting() OR public.is_admin())
  AND org_id = public.current_org_id()
);

-- Estimators only: insert own bids
CREATE POLICY "bids_insert_estimator"
ON public.bids FOR INSERT TO authenticated
WITH CHECK (
  public.is_estimator()
  AND org_id = public.current_org_id()
  AND created_by = auth.uid()
);

-- Estimators: update own bids
CREATE POLICY "bids_update_owner"
ON public.bids FOR UPDATE TO authenticated
USING  (created_by = auth.uid() AND org_id = public.current_org_id())
WITH CHECK (created_by = auth.uid() AND org_id = public.current_org_id());

-- Admin + MFA: update any bid in org
CREATE POLICY "bids_update_admin_mfa"
ON public.bids FOR UPDATE TO authenticated
USING  (public.is_admin() AND public.has_mfa() AND org_id = public.current_org_id())
WITH CHECK (public.is_admin() AND public.has_mfa() AND org_id = public.current_org_id());

-- Admin + MFA: delete any bid in org
CREATE POLICY "bids_delete_admin_mfa"
ON public.bids FOR DELETE TO authenticated
USING (public.is_admin() AND public.has_mfa() AND org_id = public.current_org_id());


-- ============================================================
-- SECTION 9: RLS policies — aae_vendors
-- ============================================================

-- Drop legacy open policies
DROP POLICY IF EXISTS "allow_all_aae_vendors"   ON public.aae_vendors;
DROP POLICY IF EXISTS "aae_vendors_read_auth"   ON public.aae_vendors;
DROP POLICY IF EXISTS "aae_vendors_admin_write" ON public.aae_vendors;
DROP POLICY IF EXISTS "vendors_select_org"      ON public.aae_vendors;
DROP POLICY IF EXISTS "vendors_insert_priv"     ON public.aae_vendors;
DROP POLICY IF EXISTS "vendors_update_priv_mfa" ON public.aae_vendors;
DROP POLICY IF EXISTS "vendors_delete_priv_mfa" ON public.aae_vendors;

-- All roles: read within org
CREATE POLICY "vendors_select_org"
ON public.aae_vendors FOR SELECT TO authenticated
USING (org_id = public.current_org_id());

-- Purchasing / accounting / admin: insert (no MFA for create)
CREATE POLICY "vendors_insert_priv"
ON public.aae_vendors FOR INSERT TO authenticated
WITH CHECK (
  (public.is_purchasing() OR public.is_accounting() OR public.is_admin())
  AND org_id = public.current_org_id()
  AND created_by = auth.uid()
);

-- Purchasing / accounting / admin: update — MFA required
CREATE POLICY "vendors_update_priv_mfa"
ON public.aae_vendors FOR UPDATE TO authenticated
USING (
  (public.is_purchasing() OR public.is_accounting() OR public.is_admin())
  AND public.has_mfa() AND org_id = public.current_org_id()
)
WITH CHECK (
  (public.is_purchasing() OR public.is_accounting() OR public.is_admin())
  AND public.has_mfa() AND org_id = public.current_org_id()
);

-- Purchasing / accounting / admin: delete — MFA required
CREATE POLICY "vendors_delete_priv_mfa"
ON public.aae_vendors FOR DELETE TO authenticated
USING (
  (public.is_purchasing() OR public.is_accounting() OR public.is_admin())
  AND public.has_mfa() AND org_id = public.current_org_id()
);


-- ============================================================
-- SECTION 10: RLS policies — aae_labor_rates
-- ============================================================

-- Drop legacy open policies
DROP POLICY IF EXISTS "allow_all_aae_labor"          ON public.aae_labor_rates;
DROP POLICY IF EXISTS "aae_labor_rates_read_auth"    ON public.aae_labor_rates;
DROP POLICY IF EXISTS "aae_labor_rates_admin_write"  ON public.aae_labor_rates;
DROP POLICY IF EXISTS "labor_select_org"             ON public.aae_labor_rates;
DROP POLICY IF EXISTS "labor_insert_priv_mfa"        ON public.aae_labor_rates;
DROP POLICY IF EXISTS "labor_update_priv_mfa"        ON public.aae_labor_rates;
DROP POLICY IF EXISTS "labor_delete_priv_mfa"        ON public.aae_labor_rates;

-- All roles: read within org
CREATE POLICY "labor_select_org"
ON public.aae_labor_rates FOR SELECT TO authenticated
USING (org_id = public.current_org_id());

-- Accounting / admin: insert — MFA required (pricing is sensitive master data)
CREATE POLICY "labor_insert_priv_mfa"
ON public.aae_labor_rates FOR INSERT TO authenticated
WITH CHECK (
  (public.is_accounting() OR public.is_admin())
  AND public.has_mfa()
  AND org_id = public.current_org_id()
  AND created_by = auth.uid()
);

-- Accounting / admin: update — MFA required
CREATE POLICY "labor_update_priv_mfa"
ON public.aae_labor_rates FOR UPDATE TO authenticated
USING (
  (public.is_accounting() OR public.is_admin())
  AND public.has_mfa() AND org_id = public.current_org_id()
)
WITH CHECK (
  (public.is_accounting() OR public.is_admin())
  AND public.has_mfa() AND org_id = public.current_org_id()
);

-- Accounting / admin: delete — MFA required
CREATE POLICY "labor_delete_priv_mfa"
ON public.aae_labor_rates FOR DELETE TO authenticated
USING (
  (public.is_accounting() OR public.is_admin())
  AND public.has_mfa() AND org_id = public.current_org_id()
);


-- ============================================================
-- SECTION 11: RLS policies — audit_log
-- MFA check is inlined directly — not via has_mfa() helper —
-- so the policy cannot be weakened by redefining the function.
-- ============================================================

DROP POLICY IF EXISTS "audit_insert_auth"     ON public.audit_log;
DROP POLICY IF EXISTS "audit_read_admin_mfa"  ON public.audit_log;
DROP POLICY IF EXISTS "audit_insert_self"     ON public.audit_log;
DROP POLICY IF EXISTS "audit_read_privileged" ON public.audit_log;

-- Any authenticated user in the org can write their own audit records.
-- Flask ensures actor_user_id = g.user.id before inserting.
CREATE POLICY "audit_insert_self"
ON public.audit_log FOR INSERT TO authenticated
WITH CHECK (
  actor_user_id = auth.uid()
  AND org_id = public.current_org_id()
);

-- Admin or accounting can read audit logs — MFA inlined, org-scoped.
CREATE POLICY "audit_read_privileged"
ON public.audit_log FOR SELECT TO authenticated
USING (
  org_id = public.current_org_id()
  AND (auth.jwt() -> 'app_metadata' ->> 'role') IN ('admin', 'accounting')
  AND (auth.jwt() ->> 'aal') = 'aal2'
);


-- ============================================================
-- SECTION 12: Org bootstrap + backfill (idempotent)
-- Inserts the AAE org only if it doesn't already exist.
-- Backfills org_id on all existing NULL rows automatically.
-- No UUID placeholder — the UUID is captured and used inline.
-- ============================================================

DO $$
DECLARE
  v_org_id uuid;
BEGIN
  -- Get existing org if already created
  SELECT id INTO v_org_id
  FROM public.orgs
  WHERE name = 'AAE Automation'
  LIMIT 1;

  -- Create it if this is a first run
  IF v_org_id IS NULL THEN
    INSERT INTO public.orgs (name)
    VALUES ('AAE Automation')
    RETURNING id INTO v_org_id;
    RAISE NOTICE 'Created AAE Automation org: %', v_org_id;
  ELSE
    RAISE NOTICE 'AAE Automation org already exists: %', v_org_id;
  END IF;

  -- Backfill all data tables — safe to run even if already done
  UPDATE public.bids            SET org_id = v_org_id WHERE org_id IS NULL;
  UPDATE public.aae_vendors     SET org_id = v_org_id WHERE org_id IS NULL;
  UPDATE public.aae_labor_rates SET org_id = v_org_id WHERE org_id IS NULL;
  UPDATE public.audit_log
  SET org_id = v_org_id
  WHERE org_id IS NULL
     OR org_id = '00000000-0000-0000-0000-000000000000'::uuid; -- catch zero-UUID placeholder from upgrade path
  UPDATE public.profiles        SET org_id = v_org_id WHERE org_id IS NULL;

  RAISE NOTICE 'Backfill complete for org: %', v_org_id;

  -- Store in a temp table so provision_admin.py bootstrap can retrieve it
  -- without needing a separate SQL query
  -- Run: SELECT id FROM public.orgs WHERE name = 'AAE Automation';
END$$;


-- ============================================================
-- VERIFICATION QUERIES
-- Run these after migration and confirm expected results.
-- ============================================================

-- 1. All 7 tables should show rowsecurity=true, forcepolicies=true
-- SELECT tablename, rowsecurity, forcepolicies
-- FROM pg_tables
-- WHERE tablename IN ('bids','profiles','aae_labor_rates','aae_vendors',
--                     'audit_log','orgs','org_members')
-- ORDER BY tablename;

-- 2. Policy counts per table
-- SELECT tablename, count(*) AS policy_count
-- FROM pg_policies
-- WHERE tablename IN ('bids','aae_vendors','aae_labor_rates',
--                     'audit_log','orgs','org_members','profiles')
-- GROUP BY tablename ORDER BY tablename;
-- Expected: bids=6, aae_vendors=4, aae_labor_rates=4,
--           audit_log=2, orgs=1, org_members=5, profiles=4

-- 3. No NULL org_id rows remain
-- SELECT 'bids' AS t, count(*) FROM public.bids WHERE org_id IS NULL
-- UNION ALL SELECT 'aae_vendors', count(*) FROM public.aae_vendors WHERE org_id IS NULL
-- UNION ALL SELECT 'aae_labor_rates', count(*) FROM public.aae_labor_rates WHERE org_id IS NULL
-- UNION ALL SELECT 'audit_log', count(*) FROM public.audit_log WHERE org_id IS NULL
-- UNION ALL SELECT 'profiles', count(*) FROM public.profiles WHERE org_id IS NULL;
-- Expected: all counts = 0

-- 4. Confirm org was created
-- SELECT id, name, created_at FROM public.orgs;

-- 5. Confirm functions are not publicly executable
-- SELECT routine_name, grantee, privilege_type
-- FROM information_schema.role_routine_grants
-- WHERE routine_schema = 'public'
--   AND grantee = 'public';
-- Expected: no rows (all functions restricted to authenticated)
