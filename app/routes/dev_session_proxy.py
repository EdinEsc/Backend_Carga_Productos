# app/routes/dev_session_proxy.py
from fastapi import APIRouter, HTTPException, Depends, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import requests

router = APIRouter(prefix="/dev", tags=["DEV Proxy"])
security = HTTPBearer()

ACL_BASE = "https://acl.casamarketapp.com"

@router.get("/commerce")
def dev_commerce(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    languages: str = Query(default="es"),
):
    token = credentials.credentials
    url = f"{ACL_BASE}/api/sidebar/quipuadmin/resource/commerce"

    print(f"[DEV COMMERCE] Enviando solicitud a ACL: {url}")
    print(f"[DEV COMMERCE] Usando token: {token[:10]}...")  # solo los primeros 10 caracteres por seguridad

    try:
        r = requests.get(
            url,
            params={"languages": languages},
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {token}",
            },
            timeout=20,
        )
    except requests.RequestException as e:
        raise HTTPException(
            status_code=502,
            detail={
                "msg": "ACL request failed",
                "error": str(e),
                "url": url,
            },
        )

    # Manejo explícito de errores comunes
    if r.status_code == 401:
        raise HTTPException(
            status_code=401,
            detail={
                "msg": "Token inválido o expirado",
                "body": r.text,
            },
        )

    if r.status_code == 403:
        raise HTTPException(
            status_code=403,
            detail={
                "msg": "Acceso denegado: tu usuario no tiene permisos de Administrador para este endpoint",
                "body": r.text,
            },
        )

    if r.status_code != 200:
        raise HTTPException(
            status_code=r.status_code,
            detail={
                "msg": "ACL non-200 response",
                "status": r.status_code,
                "url": str(r.url),
                "body": r.text,
            },
        )

    return r.json()