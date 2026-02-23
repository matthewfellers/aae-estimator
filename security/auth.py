"""
AAE Estimator — Server-side JWT Auth (Supabase RS256 JWKS) v3.0

Key design decisions (unchanged from v2.3):
- Every route handler uses get_user_sb() — user-scoped Supabase client
  so RLS policies see auth.uid() and auth.jwt() correctly.
- Role AND org_id are read from JWT app_metadata (not DB queries).
  Eliminates RLS recursion risk. provision_admin.py keeps app_metadata
  and org_members table in sync.

v3.0 additions:
- org_id now extracted from app_metadata alongside role
- require_role() decorator for purchasing/accounting gating
- accounting and manufacturing added to valid roles
- current_role() helper exposed for Flask route use
"""
import os, time, json, requests
from functools import wraps
import jwt
from flask import request, jsonify, g

SUPABASE_URL      = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "")

# Resolved at request time (not module load) so Railway env vars are
# guaranteed to be present. Module-load resolution caused EXPECTED_ISSUER
# to be "/auth/v1" when the dyno started before env vars were injected.
def _supabase_url() -> str:
    return os.environ.get("SUPABASE_URL", "").rstrip("/")

def _jwks_url() -> str:
    return f"{_supabase_url()}/auth/v1/.well-known/jwks.json"

def _expected_issuer() -> str:
    return f"{_supabase_url()}/auth/v1"

_VALID_ROLES = {"admin", "estimator", "purchasing", "accounting", "manufacturing", "viewer"}

_jwks_cache = {"ts": 0, "keys": None}

def _get_jwks() -> dict:
    now = int(time.time())
    if _jwks_cache["keys"] and (now - _jwks_cache["ts"] < 3600):
        return _jwks_cache["keys"]
    try:
        r = requests.get(_jwks_url(), timeout=10)
        r.raise_for_status()
        _jwks_cache["keys"] = r.json()
        _jwks_cache["ts"] = now
        return _jwks_cache["keys"]
    except Exception as e:
        if _jwks_cache["keys"]:
            return _jwks_cache["keys"]
        raise RuntimeError(f"Cannot fetch JWKS: {e}") from e

def verify_supabase_jwt(token: str) -> dict:
    """Verify RS256 JWT via JWKS. Validates signature AND issuer."""
    jwks = _get_jwks()
    kid = jwt.get_unverified_header(token).get("kid")
    key_data = next((k for k in jwks.get("keys", []) if k.get("kid") == kid), None)
    if not key_data:
        raise ValueError(f"JWT kid '{kid}' not found in JWKS")
    public_key = jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(key_data))
    return jwt.decode(
        token, public_key,
        algorithms=["RS256"],
        options={"verify_aud": False},
        issuer=_expected_issuer(),
    )

def get_bearer_token() -> str | None:
    auth = request.headers.get("Authorization", "")
    return auth.split(" ", 1)[1].strip() if auth.startswith("Bearer ") else None

def get_user_sb():
    """
    Returns a per-request Supabase client carrying the user's JWT.
    Required so RLS policies evaluate auth.uid(), is_admin(),
    current_org_id() etc. as the actual authenticated user.
    The global anon-key client must NEVER be used in route handlers.
    """
    from supabase import create_client
    token = getattr(g, "_bearer_token", None)
    if not token:
        raise RuntimeError("get_user_sb() called outside an authenticated request context")
    client = create_client(_supabase_url(), os.environ.get("SUPABASE_ANON_KEY", ""))
    client.postgrest.auth(token)
    return client

def _extract_claims(payload: dict) -> dict:
    """
    Read role and org_id from JWT app_metadata.
    Falls back to 'viewer' / None so RLS fails closed if not provisioned.
    Both values must be set by provision_admin.py for the user to function.
    """
    app_meta = payload.get("app_metadata", {}) or {}
    role = app_meta.get("role", "viewer")
    if role not in _VALID_ROLES:
        role = "viewer"
    org_id = app_meta.get("org_id") or None
    return {"role": role, "org_id": org_id}

def require_auth(fn):
    """
    Decorator: verifies JWT, populates g.user with:
      id, email, aal, role, org_id
    Also stores token on g._bearer_token for get_user_sb().
    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        token = get_bearer_token()
        if not token:
            return jsonify({"error": "Authentication required"}), 401
        try:
            payload = verify_supabase_jwt(token)
        except jwt.ExpiredSignatureError:
            return jsonify({"error": "Session expired — please sign in again"}), 401
        except jwt.InvalidIssuerError:
            return jsonify({"error": "Invalid token issuer"}), 401
        except Exception as e:
            return jsonify({"error": f"Invalid token: {str(e)}"}), 401

        user_id = payload.get("sub")
        if not user_id:
            return jsonify({"error": "Invalid token: missing subject"}), 401

        claims = _extract_claims(payload)
        g._bearer_token = token
        g.user = {
            "id":     user_id,
            "email":  payload.get("email", ""),
            "aal":    payload.get("aal", "aal1"),
            "role":   claims["role"],
            "org_id": claims["org_id"],
        }
        return fn(*args, **kwargs)
    return wrapper

def require_admin(require_mfa: bool = True):
    """
    Requires admin role. MFA required by default.
    Role and org_id are from JWT app_metadata — no DB query.
    """
    def decorator(fn):
        @wraps(fn)
        @require_auth
        def wrapper(*args, **kwargs):
            if g.user.get("role") != "admin":
                return jsonify({"error": "Admin access required"}), 403
            if require_mfa and g.user.get("aal") != "aal2":
                return jsonify({"error": "MFA verification required for this action"}), 403
            return fn(*args, **kwargs)
        return wrapper
    return decorator

def require_role(*roles: str, require_mfa: bool = False):
    """
    Decorator factory: allows one or more roles.
    Use for routes accessible to purchasing, accounting, admin etc.

    Usage:
        @require_role("admin", "purchasing", "accounting")
        @require_role("admin", "accounting", require_mfa=True)
    """
    role_set = set(roles)
    def decorator(fn):
        @wraps(fn)
        @require_auth
        def wrapper(*args, **kwargs):
            if g.user.get("role") not in role_set:
                return jsonify({"error": f"Access requires one of: {', '.join(sorted(role_set))}"}), 403
            if require_mfa and g.user.get("aal") != "aal2":
                return jsonify({"error": "MFA verification required for this action"}), 403
            return fn(*args, **kwargs)
        return wrapper
    return decorator
