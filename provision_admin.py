#!/usr/bin/env python3
"""
AAE ERP — Admin Provisioning Script v3.0
=========================================
AUTHORITATIVE way to create and provision users.
Sets app_metadata.role AND app_metadata.org_id in a single operation,
verifies both persisted, then upserts profiles + org_members rows.

WHY BOTH app_metadata FIELDS ARE REQUIRED (v3.0):
  - app_metadata.role   → is_admin(), is_purchasing() etc. in RLS + Flask
  - app_metadata.org_id → current_org_id() in RLS (org-scoped policies)
  If either is missing, RLS fails closed — user gets 403 or sees no data.

USAGE:
  export SUPABASE_URL="https://your-project.supabase.co"
  export SUPABASE_SERVICE_ROLE_KEY="eyJ..."   # service role — NOT anon key
  export SUPABASE_ANON_KEY="eyJ..."

  python3 provision_admin.py \
    --email mfellers@aaeautomation.com \
    --display "Matthew Fellers" \
    --role admin \
    --org-id <UUID from orgs table>

VALID ROLES: admin, estimator, purchasing, accounting, manufacturing, viewer,
              shop_employee, shop_lead, supervisor

GETTING THE ORG UUID:
  Run in Supabase SQL Editor:
    SELECT id FROM public.orgs WHERE name = 'AAE Automation';
  First-time setup (run once):
    INSERT INTO public.orgs (name) VALUES ('AAE Automation') RETURNING id;

SECURITY: SERVICE_ROLE_KEY bypasses all RLS. Never commit it to git.
Never put it in Railway. Use locally only. Safe to re-run (idempotent).
"""
import os, sys, argparse, requests

SUPABASE_URL     = os.environ.get("SUPABASE_URL", "").rstrip("/")
SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
ANON_KEY         = os.environ.get("SUPABASE_ANON_KEY", "")

VALID_ROLES = {"admin","estimator","purchasing","accounting","manufacturing","viewer",
               "shop_employee","shop_lead","supervisor"}

def check_env():
    missing = [v for v in ["SUPABASE_URL","SUPABASE_SERVICE_ROLE_KEY","SUPABASE_ANON_KEY"]
               if not os.environ.get(v)]
    if missing:
        print(f"ERROR: Missing env vars: {', '.join(missing)}")
        for v in missing: print(f"  export {v}='...'")
        sys.exit(1)

def svc():
    return {"Authorization": f"Bearer {SERVICE_ROLE_KEY}",
            "apikey": SERVICE_ROLE_KEY, "Content-Type": "application/json"}

def find_user(email):
    r = requests.get(f"{SUPABASE_URL}/auth/v1/admin/users?per_page=1000", headers=svc(), timeout=15)
    if r.status_code != 200: print(f"ERROR listing users: {r.text}"); sys.exit(1)
    data = r.json(); users = data.get("users", data) if isinstance(data, dict) else data
    m = next((u for u in users if u.get("email","").lower() == email.lower()), None)
    if not m: print(f"ERROR: '{email}' not found. Invite user first."); sys.exit(1)
    return m

def set_app_metadata(uid, role, org_id):
    r = requests.put(f"{SUPABASE_URL}/auth/v1/admin/users/{uid}",
                     headers=svc(), json={"app_metadata": {"role": role, "org_id": org_id}}, timeout=15)
    if r.status_code not in (200, 204): print(f"ERROR setting app_metadata: {r.text}"); sys.exit(1)

def verify_app_metadata(uid, role, org_id):
    r = requests.get(f"{SUPABASE_URL}/auth/v1/admin/users/{uid}", headers=svc(), timeout=15)
    if r.status_code != 200: print(f"ERROR re-fetching user: {r.text}"); sys.exit(1)
    meta = r.json().get("app_metadata", {}) or {}
    errs = []
    if meta.get("role") != role: errs.append(f"role mismatch: got '{meta.get('role')}'")
    if meta.get("org_id") != org_id: errs.append(f"org_id mismatch: got '{meta.get('org_id')}'")
    if errs:
        print(f"ERROR: Verification failed — {'; '.join(errs)}")
        print("       Is SUPABASE_SERVICE_ROLE_KEY correct (not the anon key)?"); sys.exit(1)
    return meta["role"], meta["org_id"]

def upsert_profiles(uid, email, role, display_name, org_id=None):
    row = {"user_id": uid, "email": email, "role": role, "display_name": display_name}
    if org_id:
        row["org_id"] = org_id
    r = requests.post(f"{SUPABASE_URL}/rest/v1/profiles",
                      headers={**svc(), "Prefer": "resolution=merge-duplicates"},
                      json=row,
                      timeout=15)
    if r.status_code not in (200, 201, 204):
        print(f"WARNING: profiles upsert {r.status_code}: {r.text}")
        print("         app_metadata is set correctly. Run SQL bootstrap as fallback."); return False
    return True

def upsert_org_member(uid, org_id, role):
    r = requests.post(f"{SUPABASE_URL}/rest/v1/org_members",
                      headers={**svc(), "Prefer": "resolution=merge-duplicates"},
                      json={"org_id": org_id, "user_id": uid, "role": role, "is_active": True},
                      timeout=15)
    if r.status_code not in (200, 201, 204):
        print(f"WARNING: org_members upsert {r.status_code}: {r.text}")
        print("         Manually insert org_members row if needed."); return False
    return True

def main():
    p = argparse.ArgumentParser(description="Provision an AAE ERP user")
    p.add_argument("--email",   required=True)
    p.add_argument("--display", default="")
    p.add_argument("--role",    default="estimator", choices=sorted(VALID_ROLES))
    p.add_argument("--org-id",  required=True, dest="org_id", help="UUID from public.orgs table")
    a = p.parse_args()

    check_env()
    email, role, org_id = a.email.strip().lower(), a.role, a.org_id.strip()
    display = a.display or email

    print(f"\n── AAE ERP Provisioning ─────────────────────────────────")
    print(f"  Email:   {email}  |  Role: {role}  |  Org: {org_id}\n")

    print("Step 1/5 — Finding user..."); u = find_user(email); uid = u["id"]; print(f"          {uid}")
    print(f"Step 2/5 — Setting app_metadata..."); set_app_metadata(uid, role, org_id); print("          Done.")
    print(f"Step 3/5 — Verifying..."); r, o = verify_app_metadata(uid, role, org_id); print(f"          role='{r}', org_id='{o}' ✓")
    print(f"Step 4/5 — Upserting profiles..."); ok = upsert_profiles(uid, email, role, display, org_id); ok and print("          Done. ✓")
    print(f"Step 5/5 — Upserting org_members..."); ok = upsert_org_member(uid, org_id, role); ok and print("          Done. ✓")

    print(f"\n── Done: {email} → role='{role}' org='{org_id}'")
    print("   User must sign out + back in to get refreshed JWT.")
    if role == "admin": print("   Admin write ops also require MFA enrollment.")
    print()

if __name__ == "__main__": main()
