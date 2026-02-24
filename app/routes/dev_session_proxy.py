from fastapi import APIRouter, HTTPException
import requests
import logging

router = APIRouter(prefix="/dev", tags=["DEV Proxy"])
logger = logging.getLogger(__name__)

# Credenciales correctas
CREDENTIALS = {
    "email": "soporte@edi.com",
    "password": "mb2525",
    "codeApp": "quipuadmin"
}

@router.post("/login-completo", summary="Login + Datos de usuario")
async def login_completo():
    """
    **PROCESO COMPLETO: Login en ACL + Datos del usuario**
    
    Usa las credenciales:
    - email: soporte@edi.com
    - password: mb2525
    - codeApp: quipuadmin
    """
    
    logger.info("🔄 Iniciando login con credenciales correctas...")
    logger.info(f"📧 Email: {CREDENTIALS['email']}")
    logger.info(f"📱 codeApp: {CREDENTIALS['codeApp']}")
    
    try:
        # PASO 1: Login en ACL
        logger.info("📡 PASO 1: Autenticando en ACL...")
        auth_response = requests.post(
            "https://acl.casamarketapp.com/api/authenticate",
            json=CREDENTIALS,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        
        logger.info(f"📊 Status ACL: {auth_response.status_code}")
        
        if auth_response.status_code != 200:
            return {
                "error": "Login falló",
                "status": auth_response.status_code,
                "respuesta": auth_response.text
            }
        
        auth_data = auth_response.json()
        token = auth_data.get("token")
        
        logger.info(f"✅ Token obtenido: {token[:20]}...")
        logger.info(f"👤 Usuario ACL: {auth_data.get('codeUser')}")
        
        # PASO 2: Obtener datos completos del usuario
        logger.info("📡 PASO 2: Obteniendo datos del usuario...")
        user_response = requests.get(
            "https://n4.sales.casamarketapp.com/employees/current",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.appv1.10.1+json"
            },
            timeout=10
        )
        
        if user_response.status_code != 200:
            return {
                "success": False,
                "token": token,
                "error_usuario": user_response.text
            }
        
        user_data = user_response.json()
        
        logger.info(f"✅ Usuario: {user_data.get('name')}")
        logger.info(f"✅ Email: {user_data.get('email')}")
        logger.info(f"✅ Empresa: {user_data.get('company', {}).get('companyName')}")
        
        # RESPUESTA COMPLETA
        return {
            "success": True,
            "message": "Login completado exitosamente",
            "token": token,
            "auth": {
                "codeUser": auth_data.get("codeUser"),
                "codeProject": auth_data.get("codeProject"),
                "domains": auth_data.get("domains", [])
            },
            "user": {
                "id": user_data.get("id"),
                "name": user_data.get("name"),
                "email": user_data.get("email"),
                "company": user_data.get("company", {}).get("companyName"),
                "warehouse": user_data.get("warehouse", {}).get("name"),
                "cash": user_data.get("cash", {}).get("name"),
                "subsidiary": user_data.get("subsidiary", {}).get("sucursalName"),
                "role": user_data.get("roleCode"),
                "full_data": user_data  # Todos los datos completos
            }
        }
        
    except requests.Timeout:
        logger.error("❌ Timeout en la conexión")
        return {"error": "Timeout en la conexión"}
        
    except requests.RequestException as e:
        logger.error(f"❌ Error de conexión: {str(e)}")
        return {"error": f"Error de conexión: {str(e)}"}
        
    except Exception as e:
        logger.error(f"❌ Error inesperado: {str(e)}")
        return {"error": str(e)}

@router.post("/login", summary="Solo login en ACL")
async def login_solo():
    """
    **Solo prueba de login en ACL**
    """
    try:
        response = requests.post(
            "https://acl.casamarketapp.com/api/authenticate",
            json=CREDENTIALS,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        
        if response.status_code == 200:
            return {
                "success": True,
                "data": response.json()
            }
        else:
            return {
                "success": False,
                "status": response.status_code,
                "error": response.text
            }
            
    except Exception as e:
        return {"error": str(e)}

@router.get("/test", summary="Endpoint de prueba")
async def test():
    """
    **Verifica que el router funciona**
    """
    return {
        "message": "✅ dev_session_proxy funcionando",
        "credenciales_configuradas": {
            "email": CREDENTIALS["email"],
            "codeApp": CREDENTIALS["codeApp"],
            "password": "********"  # Ocultada por seguridad
        }
    }