-- ═══════════════════════════════════════════════════════════════════════════════
-- Shipping Sticker Library + library_template_id on doc rules
-- ═══════════════════════════════════════════════════════════════════════════════

-- 1. Sticker library table
CREATE TABLE IF NOT EXISTS public.shipping_sticker_library (
    id              bigserial PRIMARY KEY,
    org_id          uuid NOT NULL DEFAULT current_org_id()
                        REFERENCES public.organizations(id),
    created_by      uuid DEFAULT auth.uid()
                        REFERENCES auth.users(id),
    template_name   text NOT NULL,
    file_name       text NOT NULL,
    file_data       bytea NOT NULL,
    placeholders    jsonb NOT NULL DEFAULT '[]',
    is_deleted      boolean NOT NULL DEFAULT false,
    created_at      timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE public.shipping_sticker_library ENABLE ROW LEVEL SECURITY;

-- SELECT: all authenticated org members
CREATE POLICY shipping_sticker_library_select ON public.shipping_sticker_library
    FOR SELECT TO authenticated
    USING (org_id = current_org_id());

-- INSERT: admin only
CREATE POLICY shipping_sticker_library_insert ON public.shipping_sticker_library
    FOR INSERT TO authenticated
    WITH CHECK (
        org_id = current_org_id()
        AND current_user_role() = 'admin'
    );

-- DELETE (hard): admin only (soft-delete via RPC preferred)
CREATE POLICY shipping_sticker_library_delete ON public.shipping_sticker_library
    FOR DELETE TO authenticated
    USING (
        org_id = current_org_id()
        AND current_user_role() = 'admin'
    );

-- UPDATE: admin only (for soft-delete flag)
CREATE POLICY shipping_sticker_library_update ON public.shipping_sticker_library
    FOR UPDATE TO authenticated
    USING (
        org_id = current_org_id()
        AND current_user_role() = 'admin'
    )
    WITH CHECK (
        org_id = current_org_id()
        AND current_user_role() = 'admin'
    );

-- Grants
GRANT SELECT, INSERT, UPDATE, DELETE ON public.shipping_sticker_library TO authenticated;
GRANT USAGE, SELECT ON SEQUENCE public.shipping_sticker_library_id_seq TO authenticated;


-- 2. Soft-delete RPC
CREATE OR REPLACE FUNCTION public.soft_delete_sticker_template(p_template_id bigint)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
    UPDATE public.shipping_sticker_library
       SET is_deleted = true
     WHERE id = p_template_id
       AND org_id = current_org_id();
END;
$$;

GRANT EXECUTE ON FUNCTION public.soft_delete_sticker_template(bigint) TO authenticated;


-- 3. Add library_template_id column to shipping_kit_doc_rules
ALTER TABLE public.shipping_kit_doc_rules
    ADD COLUMN IF NOT EXISTS library_template_id bigint
        REFERENCES public.shipping_sticker_library(id);
