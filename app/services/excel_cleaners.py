import re
import unicodedata
import string
import secrets
import math
from typing import Optional, Set
import pandas as pd

IGV_FACTOR = 1.18
ROW_ID_COL_DEFAULT = "__ROW_ID__"

# ============================================================
# Normalización base (Ñ OK)
# ============================================================
def _strip_accents_keep_enye(s: str) -> str:
    if not isinstance(s, str):
        s = str(s)

    s = s.replace("Ñ", "__ENYE_MAY__").replace("ñ", "__ENYE_MIN__")
    s_norm = unicodedata.normalize("NFD", s)
    s_norm = "".join(ch for ch in s_norm if unicodedata.category(ch) != "Mn")
    s_norm = unicodedata.normalize("NFC", s_norm)
    return s_norm.replace("__ENYE_MAY__", "Ñ").replace("__ENYE_MIN__", "ñ")


def normalize_text_value(v) -> str:
    if pd.isna(v):
        return ""
    s = str(v).strip()
    if not s:
        return ""
    s = _strip_accents_keep_enye(s).upper()
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"(\d)\s*\.\s*(\d)", r"\1.\2", s)
    s = re.sub(r"(\d(?:\.\d+)?)\s*(ML|L|G|KG|MG|OZ|LB)\b", r"\1\2", s)
    return s


# ============================================================
# Limpieza específica
# ============================================================
def clean_alnum_spaces(v) -> str:
    s = normalize_text_value(v)
    s = re.sub(r"[^A-Z0-9 ]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def clean_category_value(v) -> str:
    s = clean_alnum_spaces(v)
    return s if re.search(r"[A-Z0-9]", s) else ""


# ============================================================
# "UNIDAD"
# ============================================================
_UNIT_ABBR_MAP = {
    "UND": "UNIDAD",
    "UNID": "UNIDAD",
    "UNI": "UNIDAD",
    "U": "UNIDAD",
    "PAQ": "PAQUETE",
    "PAQT": "PAQUETE",
    "PAQU": "PAQUETE",
    "BOT": "BOTELLA",
    "BT": "BOTELLA",
    "SAC": "SACO",
    "CJ": "CAJA",
    "CAJ": "CAJA",
    "BOL": "BOLSA",
}

_ALLOWED_UNITS = {"UNIDAD", "PAQUETE", "BOTELLA", "SACO", "CAJA", "BOLSA"}


def clean_unit_value(v) -> str:
    s = normalize_text_value(v)
    if not s:
        return "UNIDAD"

    s2 = re.sub(r"[.\-_/\\()]+", " ", s)
    s2 = re.sub(r"\s+", " ", s2).strip()

    has_digits = bool(re.search(r"\d", s2))
    has_non_letters = bool(re.search(r"[^A-Z Ñ ]", s2))

    tokens = [t for t in s2.split() if t]

    if has_digits or has_non_letters:
        for t in tokens:
            t_clean = re.sub(r"[^A-ZÑ]", "", t)
            if t_clean in _UNIT_ABBR_MAP:
                return _UNIT_ABBR_MAP[t_clean]
        return "UNIDAD"

    if len(tokens) == 1 and tokens[0] in _UNIT_ABBR_MAP:
        return _UNIT_ABBR_MAP[tokens[0]]

    candidate = " ".join(tokens).strip()
    if candidate in _ALLOWED_UNITS:
        return candidate

    return "UNIDAD"


# ============================================================
# Códigos - VERSIÓN ACTUALIZADA: MÍNIMO 4 CARACTERES
# ============================================================
ALNUM = set(string.ascii_uppercase + string.digits)

def clean_product_code(v) -> str:
    """Limpia el código manteniendo solo caracteres alfanuméricos"""
    if pd.isna(v) or str(v).strip() == "":
        return ""
    return re.sub(r"[^A-Z0-9]+", "", normalize_text_value(v))

def is_valid_product_code(code: str) -> bool:
    """
    Valida según requisitos actualizados:
    - Mínimo 4 caracteres para ser válido
    - Si tiene menos de 4 caracteres: inválido (genera nuevo)
    - Si tiene 4 o más caracteres: válido (pasa)
    """
    if not code:
        return False
    
    # Un código es válido si tiene 4 o más caracteres
    return len(code) >= 4

def generate_unique_code(existing: set[str], prefix="CM") -> str:
    """Genera código único con prefijo CM + 10 caracteres"""
    while True:
        c = prefix + "".join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(10))
        if c not in existing:
            existing.add(c)
            return c

def process_product_code(valor, existing_codes: set[str], row_id: int = None) -> dict:
    """
    Procesa un código según las reglas actualizadas y devuelve:
    - código_limpio: el código final
    - es_generico: si se generó automáticamente
    - razon: por qué se generó (vacío, menos de 4 dígitos, etc.)
    """
    resultado = {
        "codigo_original": str(valor) if pd.notna(valor) else "",
        "codigo_final": "",
        "es_generico": False,
        "razon": None
    }
    
    # Caso 1: Código vacío
    if pd.isna(valor) or not str(valor).strip():
        resultado["codigo_final"] = generate_unique_code(existing_codes)
        resultado["es_generico"] = True
        resultado["razon"] = "VACÍO"
        return resultado
    
    s = str(valor).strip().upper()
    s_limpio = re.sub(r"[^A-Z0-9]+", "", s)
    
    # Caso 2: Después de limpiar, quedó vacío
    if not s_limpio:
        resultado["codigo_final"] = generate_unique_code(existing_codes)
        resultado["es_generico"] = True
        resultado["razon"] = "CARACTERES INVÁLIDOS"
        return resultado
    
    # Caso 3: Código con menos de 4 caracteres (1, 2 o 3 dígitos)
    if len(s_limpio) < 4:
        resultado["codigo_final"] = generate_unique_code(existing_codes)
        resultado["es_generico"] = True
        resultado["razon"] = f"{len(s_limpio)} CARACTERES (mínimo 4)"
        return resultado
    
    # Caso 4: Verificar duplicados - IMPORTANTE: NO generar nuevo, solo marcar
    if s_limpio in existing_codes:
        resultado["codigo_final"] = s_limpio  # Mantenemos el duplicado
        resultado["es_generico"] = False
        resultado["razon"] = "DUPLICADO (se mantiene)"
        # Nota: No añadimos a existing_codes para que se mantenga el duplicado
        return resultado
    
    # Caso 5: Código válido (4+ caracteres, no duplicado)
    existing_codes.add(s_limpio)
    resultado["codigo_final"] = s_limpio
    resultado["es_generico"] = False
    resultado["razon"] = "VÁLIDO"
    
    return resultado


# ============================================================
# Números
# ============================================================
def to_number(v):
    if pd.isna(v) or str(v).strip() == "":
        return None
    s = str(v).replace(",", ".")
    s = re.sub(r"[^0-9.\-]", "", s)
    try:
        x = float(s)
        if math.isnan(x) or math.isinf(x):
            return None
        return x
    except Exception:
        return None


def _find_col(df: pd.DataFrame, name: str) -> Optional[str]:
    name = normalize_text_value(name)
    for c in df.columns:
        if name in normalize_text_value(c):
            return c
    return None


def _is_null(x) -> bool:
    return x is None or (isinstance(x, float) and pd.isna(x))


# ============================================================
# JSON safe helper
# ============================================================
def _json_safe(v):
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if hasattr(v, "isoformat"):
        try:
            return v.isoformat()
        except Exception:
            return str(v)
    return v


# ============================================================
# Helper: drop filas totalmente vacías
# ============================================================
def _drop_all_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    tmp = df.copy()
    for c in tmp.columns:
        if tmp[c].dtype == "object":
            tmp[c] = tmp[c].astype(str).str.strip()
    mask_all_empty = tmp.apply(lambda r: all((x is None) or (str(x).strip() == "") or (str(x).strip().upper() == "NAN") for x in r), axis=1)
    return df.loc[~mask_all_empty].copy().reset_index(drop=True)