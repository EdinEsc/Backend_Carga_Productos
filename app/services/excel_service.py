# =========================
# excel_service.py (COMPLETO - CON NOMBRE DE TIENDA DINÁMICO)
# =========================
import io
import re
import unicodedata
import string
import secrets
import math
from typing import Optional, Tuple

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
# UNIDAD (solo palabras completas)
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
    """
    - Si vacío -> UNIDAD
    - Si tiene números/símbolos -> intenta resolver abreviatura; si no, UNIDAD
    - Solo letras y espacios; sin puntos.
    - Si no está en catálogo permitido, por defecto UNIDAD (estricto).
    """
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
# Códigos
# ============================================================
ALNUM = set(string.ascii_uppercase + string.digits)


def clean_product_code(v) -> str:
    return re.sub(r"[^A-Z0-9]+", "", normalize_text_value(v))


def is_valid_product_code(code: str) -> bool:
    return (
        bool(code)
        and 4 <= len(code) <= 15
        and any(c.isdigit() for c in code)
        and all(c in ALNUM for c in code)
    )


def generate_unique_code(existing: set[str], prefix="CM") -> str:
    while True:
        c = prefix + "".join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(10))
        if c not in existing:
            existing.add(c)
            return c


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
# Duplicados por NOMBRE - SIN row_id
# ============================================================
def build_duplicate_groups(df: pd.DataFrame, col_nombre: str) -> list[dict]:
    mask = df[col_nombre].astype(str).str.strip().ne("") & df[col_nombre].duplicated(keep=False)
    dups = df.loc[mask].copy()
    if dups.empty:
        return []

    groups = []
    for name, g in dups.groupby(col_nombre, sort=True):
        raw_rows = g.to_dict(orient="records")
        rows = [{k: _json_safe(v) for k, v in r.items()} for r in raw_rows]
        groups.append({"key": str(name), "count": int(len(rows)), "rows": rows})
    return groups


# ============================================================
# Duplicados por NOMBRE - CON row_id
# ============================================================
def build_duplicate_groups_with_row_id(
    df: pd.DataFrame,
    col_nombre: str,
    row_id_col: str = ROW_ID_COL_DEFAULT,
) -> list[dict]:
    if col_nombre not in df.columns or row_id_col not in df.columns:
        return []

    s = df[col_nombre].astype(str).str.strip()
    mask = s.ne("") & df[col_nombre].duplicated(keep=False)
    dups = df.loc[mask].copy()
    if dups.empty:
        return []

    groups = []
    for name, g in dups.groupby(col_nombre, sort=True):
        raw_rows = g.to_dict(orient="records")
        rows = [{k: _json_safe(v) for k, v in r.items()} for r in raw_rows]
        groups.append({"key": str(name), "count": int(len(rows)), "rows": rows})
    return groups


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


# ============================================================
# CONVERSIÓN: construir DF desde archivo
# ============================================================
def build_conversion_df_from_file(
    file_path: str,
    header_row: int = 3,
    row_id_col: str = ROW_ID_COL_DEFAULT,
) -> pd.DataFrame:
    """
    Lee el Excel de CONVERSIÓN y devuelve un DataFrame listo:
    - Lee con header=3 (igual que tus plantillas)
    - Normaliza nombres de columnas
    - Normaliza celdas de texto
    - Elimina filas completamente vacías
    - Agrega __ROW_ID__ estable (fila Excel real empezando en 5)
    """
    df = pd.read_excel(file_path, engine="openpyxl", header=header_row)

    # normalizar columnas
    df.columns = [normalize_text_value(c) for c in df.columns]

    # normalizar textos
    for c in df.columns:
        if df[c].dtype == "object":
            df[c] = df[c].apply(normalize_text_value)

    df = _drop_all_empty_rows(df)

    # row id estable para UI (siempre 5..)
    df[row_id_col] = range(5, 5 + len(df))
    return df


# ============================================================
# NORMALIZACIÓN A DF (para /excel/analyze) - CARGA NORMAL
# ============================================================
def normalize_to_dataframe(
    excel_bytes: bytes,
    round_numeric: Optional[int] = None,
) -> tuple[pd.DataFrame, dict, dict]:
    df = pd.read_excel(io.BytesIO(excel_bytes), engine="openpyxl", header=3)
    before_rows = len(df)

    df.columns = [normalize_text_value(c) for c in df.columns]

    col_codigo = _find_col(df, "CODIGO")
    col_nombre = _find_col(df, "NOMBRE")
    col_desc = _find_col(df, "DESCRIPCION")
    col_cat = _find_col(df, "CATEGORIA")
    col_pcost = _find_col(df, "PRECIO DE COSTO")
    col_pventa = _find_col(df, "PRECIO DE VENTA")
    col_unidad = _find_col(df, "UNIDAD")
    col_stock = _find_col(df, "CANTIDAD") or _find_col(df, "STOCK")
    col_stock_min = _find_col(df, "STOCK MINIMO")
    col_marca = _find_col(df, "MARCA")
    col_modelo = _find_col(df, "MODELO")
    col_porcentaje = _find_col(df, "PORCENTAJE") or _find_col(df, "PORCENTAJE COSTO")

    for c in df.columns:
        if df[c].dtype == "object":
            df[c] = df[c].apply(normalize_text_value)

    if col_nombre:
        df[col_nombre] = df[col_nombre].apply(clean_alnum_spaces)
    if col_desc:
        df[col_desc] = df[col_desc].apply(clean_alnum_spaces)
    if col_cat:
        df[col_cat] = df[col_cat].apply(clean_category_value)

    if col_unidad:
        df[col_unidad] = df[col_unidad].apply(clean_unit_value)
    else:
        col_unidad = "__UNIDAD__"
        df[col_unidad] = "UNIDAD"

    if col_marca:
        df[col_marca] = df[col_marca].apply(
            lambda x: "S/M" if pd.isna(x) or str(x).strip() == "" else str(x).strip()
        )
    else:
        col_marca = "__MARCA__"
        df[col_marca] = "S/M"

    if col_modelo:
        df[col_modelo] = df[col_modelo].apply(
            lambda x: "S/M" if pd.isna(x) or str(x).strip() == "" else str(x).strip()
        )
    else:
        col_modelo = "__MODELO__"
        df[col_modelo] = "S/M"

    existing = set()
    codes_fixed = 0

    def fix_code(v):
        nonlocal codes_fixed
        c = clean_product_code(v)
        if is_valid_product_code(c) and c not in existing:
            existing.add(c)
            return c
        codes_fixed += 1
        return generate_unique_code(existing)

    if col_codigo:
        df[col_codigo] = df[col_codigo].apply(fix_code)

    if col_pcost:
        df[col_pcost] = df[col_pcost].apply(to_number).apply(lambda x: 0.0 if _is_null(x) else x)
    else:
        col_pcost = "__PCOST__"
        df[col_pcost] = 0.0

    if col_pventa:
        df[col_pventa] = df[col_pventa].apply(to_number).apply(lambda x: 1.0 if _is_null(x) else x)
    else:
        col_pventa = "__PVENTA__"
        df[col_pventa] = 1.0

    if col_stock:
        df[col_stock] = df[col_stock].apply(to_number).apply(lambda x: 0.0 if _is_null(x) else x)
    else:
        col_stock = "__STOCK__"
        df[col_stock] = 0.0

    if col_stock_min:
        df[col_stock_min] = df[col_stock_min].apply(to_number)

    if col_cat:
        df[col_cat] = df[col_cat].apply(lambda x: x if str(x).strip() else "SIN CATEGORIA")
    else:
        col_cat = "__CAT__"
        df[col_cat] = "SIN CATEGORIA"

    if col_porcentaje:
        df[col_porcentaje] = df[col_porcentaje].apply(to_number).apply(
            lambda x: 18.0 if _is_null(x) or x <= 0 else x
        )
    else:
        col_porcentaje = "__PORCENTAJE__"
        df[col_porcentaje] = 18.0

    if round_numeric is not None:
        num_cols = df.select_dtypes(include=["number"]).columns
        df[num_cols] = df[num_cols].round(round_numeric)

    meta = {
        "col_codigo": col_codigo,
        "col_nombre": col_nombre,
        "col_desc": col_desc,
        "col_cat": col_cat,
        "col_pcost": col_pcost,
        "col_pventa": col_pventa,
        "col_unidad": col_unidad,
        "col_stock": col_stock,
        "col_stock_min": col_stock_min,
        "col_marca": col_marca,
        "col_modelo": col_modelo,
        "col_porcentaje": col_porcentaje,
    }

    stats = {"rows_before": int(before_rows), "codes_fixed": int(codes_fixed)}
    return df, meta, stats


# ============================================================
# FUNCIÓN PRINCIPAL (genera Excel QA) - CARGA NORMAL
# ============================================================
def normalize_excel_bytes(
    excel_bytes: bytes,
    round_numeric: Optional[int] = None,
    selected_row_ids: Optional[list[int]] = None,
    apply_igv_cost: bool = False,
    apply_igv_sale: bool = False,
    tienda_nombre: str = "Tienda1",
) -> Tuple[bytes, dict]:
    ROW_ID_COL = ROW_ID_COL_DEFAULT

    df = pd.read_excel(io.BytesIO(excel_bytes), engine="openpyxl", header=3)
    before_rows = len(df)

    df.columns = [normalize_text_value(c) for c in df.columns]

    # Row id estable para UI
    df[ROW_ID_COL] = range(5, 5 + len(df))

    col_codigo = _find_col(df, "CODIGO")
    col_nombre = _find_col(df, "NOMBRE")
    col_codigo_padre = _find_col(df, "CODIGO PADRE")
    col_codigo_alterno = _find_col(df, "CODIGO ALTERNO")
    col_codigo_barra = _find_col(df, "CODIGO BARRA")
    col_desc = _find_col(df, "DESCRIPCION")
    col_cat = _find_col(df, "CATEGORIA")
    col_pcost = _find_col(df, "PRECIO DE COSTO")
    col_pventa = _find_col(df, "PRECIO DE VENTA")
    col_unidad = _find_col(df, "UNIDAD")
    col_porcentaje = _find_col(df, "PORCENTAJE") or _find_col(df, "PORCENTAJE COSTO")
    col_marca = _find_col(df, "MARCA")
    col_modelo = _find_col(df, "MODELO")
    col_almacenable = _find_col(df, "ALMACENABLE")
    col_stock = _find_col(df, "CANTIDAD") or _find_col(df, "STOCK")
    col_stock_min = _find_col(df, "STOCK MINIMO")

    for c in df.columns:
        if df[c].dtype == "object":
            df[c] = df[c].apply(normalize_text_value)

    # 👇 CAMBIO: Para NOMBRE, solo convertir a mayúsculas sin limpieza de caracteres especiales
    if col_nombre:
        df[col_nombre] = df[col_nombre].apply(lambda x: str(x).upper() if pd.notna(x) else "")
    
    # Las demás columnas mantienen su limpieza normal
    if col_desc:
        df[col_desc] = df[col_desc].apply(clean_alnum_spaces)
    if col_cat:
        df[col_cat] = df[col_cat].apply(clean_category_value)

    if col_unidad:
        df[col_unidad] = df[col_unidad].apply(clean_unit_value)
    else:
        col_unidad = "__UNIDAD__"
        df[col_unidad] = "UNIDAD"

    if col_marca:
        df[col_marca] = df[col_marca].apply(
            lambda x: "S/M" if pd.isna(x) or str(x).strip() == "" else str(x).strip()
        )
    else:
        col_marca = "__MARCA__"
        df[col_marca] = "S/M"

    if col_modelo:
        df[col_modelo] = df[col_modelo].apply(
            lambda x: "S/M" if pd.isna(x) or str(x).strip() == "" else str(x).strip()
        )
    else:
        col_modelo = "__MODELO__"
        df[col_modelo] = "S/M"

    # PORCENTAJE ahora SIEMPRE 18
    porcentaje_default = 18.0
    if col_porcentaje:
        df[col_porcentaje] = df[col_porcentaje].apply(to_number).apply(
            lambda x: porcentaje_default if _is_null(x) or x <= 0 else x
        )
    else:
        col_porcentaje = "__PORCENTAJE__"
        df[col_porcentaje] = porcentaje_default

    # filtro duplicados por NOMBRE (selección UI)
    if selected_row_ids is not None and len(selected_row_ids) > 0 and col_nombre:
        wanted = set(int(x) for x in selected_row_ids)

        dup_mask = df[col_nombre].astype(str).str.strip().ne("") & df[col_nombre].duplicated(keep=False)
        dup_row_ids = set(df.loc[dup_mask, ROW_ID_COL].astype(int).tolist())

        keep_mask = (~df[ROW_ID_COL].isin(dup_row_ids)) | (df[ROW_ID_COL].isin(wanted))
        df = df.loc[keep_mask].copy().reset_index(drop=True)

    # Códigos:
    existing_codigo = set()
    codes_fixed = 0

    def fix_code_generate(v):
        nonlocal codes_fixed
        c = clean_product_code(v)
        if is_valid_product_code(c) and c not in existing_codigo:
            existing_codigo.add(c)
            return c
        codes_fixed += 1
        return generate_unique_code(existing_codigo, prefix="CM")

    if col_codigo:
        df[col_codigo] = df[col_codigo].apply(fix_code_generate)

    def fix_code_blank_factory():
        seen = set()

        def _fix(v):
            c = clean_product_code(v)
            if is_valid_product_code(c) and c not in seen:
                seen.add(c)
                return c
            return ""

        return _fix

    if col_codigo_barra:
        df[col_codigo_barra] = df[col_codigo_barra].apply(fix_code_blank_factory())

    if col_codigo_padre:
        df[col_codigo_padre] = df[col_codigo_padre].apply(fix_code_blank_factory())

    # Numéricos + defaults
    if col_pcost:
        df[col_pcost] = df[col_pcost].apply(to_number).apply(lambda x: 0.0 if _is_null(x) else x)
    else:
        col_pcost = "__PCOST__"
        df[col_pcost] = 0.0

    if col_pventa:
        df[col_pventa] = df[col_pventa].apply(to_number).apply(lambda x: 1.0 if _is_null(x) else x)
    else:
        col_pventa = "__PVENTA__"
        df[col_pventa] = 1.0

    if col_stock:
        df[col_stock] = df[col_stock].apply(to_number).apply(lambda x: 0.0 if _is_null(x) else x)
    else:
        col_stock = "__STOCK__"
        df[col_stock] = 0.0

    if col_stock_min:
        df[col_stock_min] = df[col_stock_min].apply(to_number)

    if col_cat:
        df[col_cat] = df[col_cat].apply(lambda x: x if str(x).strip() else "SIN CATEGORIA")
    else:
        col_cat = "__CAT__"
        df[col_cat] = "SIN CATEGORIA"

    if col_almacenable:
        df[col_almacenable] = df[col_almacenable].apply(
            lambda x: "SI" if str(x).upper() in ["SI", "S", "YES", "Y", "1", "TRUE"] else "NO"
        )
    else:
        col_almacenable = "__ALMACENABLE__"
        df[col_almacenable] = "SI"

    if round_numeric is not None:
        num_cols = df.select_dtypes(include=["number"]).columns
        df[num_cols] = df[num_cols].round(round_numeric)

    # 👇 APLICAR IGV A TODOS LOS DATOS ANTES DE LA AUDITORÍA
    df_con_igv = df.copy()
    
    if apply_igv_cost and col_pcost:
        df_con_igv[col_pcost] = df_con_igv[col_pcost].apply(lambda x: x * IGV_FACTOR if not _is_null(x) else x)
    
    if apply_igv_sale and col_pventa:
        df_con_igv[col_pventa] = df_con_igv[col_pventa].apply(lambda x: x * IGV_FACTOR if not _is_null(x) else x)

    # Auditoría + correcciones (usando df_con_igv como base)
    errores = []
    ok_mask = []
    corrected = df_con_igv.copy()

    def push_error(i, codigo, colname, valor, err, solucion, comentario):
        errores.append(
            {
                "Código": codigo,
                "Ubicación (Fila / Columna)": f"{i+2} / {colname}",
                "Valor Detectado con error": valor,
                "Errores Detectados": err,
                "Solución Sugerida (Dato Listo)": solucion,
                "Comentarios": comentario,
            }
        )

    for i in range(len(df_con_igv)):
        ok = True

        codigo = df_con_igv.at[i, col_codigo] if col_codigo else ""
        nombre = df_con_igv.at[i, col_nombre] if col_nombre else ""
        unidad = df_con_igv.at[i, col_unidad] if col_unidad else ""
        categoria = df_con_igv.at[i, col_cat] if col_cat else "SIN CATEGORIA"

        pc = float(df_con_igv.at[i, col_pcost])
        pv = float(df_con_igv.at[i, col_pventa])
        st = float(df_con_igv.at[i, col_stock])

        if not str(codigo).strip():
            ok = False
            push_error(i, codigo, col_codigo or "CODIGO", "", "CÓDIGO VACÍO", codigo, "Código es obligatorio.")

        if not str(nombre).strip():
            ok = False
            push_error(i, codigo, col_nombre or "NOMBRE", "", "NOMBRE VACÍO", "", "Nombre es obligatorio.")

        if not str(unidad).strip():
            ok = False
            push_error(i, codigo, col_unidad or "UNIDAD", "", "UNIDAD VACÍA", "UNIDAD", "Unidad es obligatoria. Se asigna UNIDAD.")

        if str(categoria).strip() == "SIN CATEGORIA":
            push_error(i, codigo, col_cat or "CATEGORIA", "", "CATEGORÍA VACÍA -> DEFAULT", "SIN CATEGORIA", "Se asignó default por categoría vacía/ inválida.")

        if st < 0:
            ok = False
            corrected.at[i, col_stock] = 0.0
            push_error(i, codigo, col_stock, st, "STOCK NEGATIVO", 0.0, "Stock no puede ser negativo. Se ajustó a 0.")

        if pc < 0:
            ok = False
            corrected.at[i, col_pcost] = 0.0
            push_error(i, codigo, col_pcost, pc, "PRECIO COSTO < 0", 0.0, "Costo mínimo 0. Se ajustó a 0.")

        if pv < 1:
            ok = False
            corrected.at[i, col_pventa] = 1.0
            push_error(i, codigo, col_pventa, pv, "PRECIO VENTA < 1", 1.0, "Venta mínima 1. Se ajustó a 1.")

        pc2 = float(corrected.at[i, col_pcost])
        pv2 = float(corrected.at[i, col_pventa])
        if pv2 <= pc2:
            ok = False
            push_error(i, codigo, col_pventa, pv2, "PRECIO VENTA <= PRECIO COSTO", pv2, "Regla: venta debe ser mayor que costo. No se ajusta automático.")

        ok_mask.append(ok)

    errores_df = pd.DataFrame(
        errores,
        columns=[
            "Código",
            "Ubicación (Fila / Columna)",
            "Valor Detectado con error",
            "Errores Detectados",
            "Solución Sugerida (Dato Listo)",
            "Comentarios",
        ],
    )

    productos_ok = df_con_igv[pd.Series(ok_mask)].copy()
    productos_corregidos = corrected[~pd.Series(ok_mask)].copy()

    # eliminar row_id antes de escribir
    for dfx in (df_con_igv, corrected, productos_ok, productos_corregidos):
        if ROW_ID_COL in dfx.columns:
            dfx.drop(columns=[ROW_ID_COL], inplace=True)

    final_df = pd.concat([productos_ok, productos_corregidos], ignore_index=True)

    # Plantilla API - CON EL MISMO ORDEN DE SIEMPRE
    codigo_padre_default = ""
    codigo_alterno_default = ""
    r_lista1_default = "0-0-0"
    w_tienda1_default = final_df[col_stock] if col_stock else 0

    # 👇 Usar el nombre de la tienda para la columna
    nombre_columna_tienda = f"W-{tienda_nombre}"

    plantilla_api = pd.DataFrame(
        {
            "Nombre": final_df[col_nombre] if col_nombre else "",
            "Descripcion": final_df[col_desc] if col_desc else "",
            "codigo padre": final_df[col_codigo_padre] if col_codigo_padre else codigo_padre_default,
            "codigo": final_df[col_codigo] if col_codigo else "",
            "Codigo alterno": final_df[col_codigo_alterno] if col_codigo_alterno else codigo_alterno_default,
            "codigo barra": final_df[col_codigo_barra] if col_codigo_barra else "",
            "Categoria": final_df[col_cat],
            "stock": final_df[col_stock],
            "stock minimo": final_df[col_stock_min] if col_stock_min else "",
            "precio costo": final_df[col_pcost],
            "precio venta": final_df[col_pventa],
            "porcentaje costo": final_df[col_porcentaje] if col_porcentaje else 18.0,
            "R-Lista1": r_lista1_default,
            "unidad": final_df[col_unidad] if col_unidad else "",
            "Marca": final_df[col_marca] if col_marca else "S/M",
            "Modelo": final_df[col_modelo] if col_modelo else "S/M",
            "Almacenable": final_df[col_almacenable] if col_almacenable else "SI",
            nombre_columna_tienda: w_tienda1_default,
        }
    )

    # 👇 Lista de columnas en orden exacto
    columnas_ordenadas = [
        "Nombre",
        "Descripcion",
        "codigo padre",
        "codigo",
        "Codigo alterno",
        "codigo barra",
        "Categoria",
        "stock",
        "stock minimo",
        "precio costo",
        "precio venta",
        "porcentaje costo",
        "R-Lista1",
        "unidad",
        "Marca",
        "Modelo",
        "Almacenable",
        nombre_columna_tienda,
    ]
    
    plantilla_api = plantilla_api[columnas_ordenadas]

    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        errores_df.to_excel(w, index=False, sheet_name="Errores_Detectados")
        productos_ok.to_excel(w, index=False, sheet_name="Productos_OK")
        productos_corregidos.to_excel(w, index=False, sheet_name="Productos_Corregidos")
        plantilla_api.to_excel(w, index=False, sheet_name="productos")

    stats = {
        "rows_before": int(before_rows),
        "rows_ok": int(len(productos_ok)),
        "rows_corrected": int(len(productos_corregidos)),
        "errors_count": int(len(errores_df)),
        "codes_fixed": int(codes_fixed),
    }

    return out.getvalue(), stats






# ============================================================
# CARGA POR CONVERSIÓN: Limpieza del output
# ============================================================
def clean_conversion_output_df(
    df_final: pd.DataFrame,
    apply_igv_cost: bool = False,
    apply_igv_sale: bool = False,
    round_numeric: Optional[int] = None,
    tienda_nombre: str = "Tienda1",  # 👈 NUEVO PARÁMETRO
) -> tuple[pd.DataFrame, dict]:
    df = df_final.copy()
    df.columns = [normalize_text_value(c) for c in df.columns]

    col_nombre = _find_col(df, "NOMBRE")
    col_desc = _find_col(df, "DESCRIPCION")
    col_cat = _find_col(df, "CATEGORIA")

    # CODIGO exacto
    col_codigo = None
    for c in df.columns:
        if normalize_text_value(c) == "CODIGO":
            col_codigo = c
            break
    if col_codigo is None:
        col_codigo = _find_col(df, "CODIGO")

    # CODIGO BARRA / CODIGO PADRE
    col_codigo_barra = None
    for c in df.columns:
        if normalize_text_value(c) == "CODIGO BARRA":
            col_codigo_barra = c
            break
    if col_codigo_barra is None:
        col_codigo_barra = _find_col(df, "CODIGO BARRA")

    col_codigo_padre = None
    for c in df.columns:
        if normalize_text_value(c) == "CODIGO PADRE":
            col_codigo_padre = c
            break
    if col_codigo_padre is None:
        col_codigo_padre = _find_col(df, "CODIGO PADRE")

    col_unidad = _find_col(df, "UNIDAD")
    col_marca = _find_col(df, "MARCA")
    col_modelo = _find_col(df, "MODELO")
    col_pcost = _find_col(df, "PRECIO COSTO")
    col_pventa = _find_col(df, "PRECIO VENTA")
    col_stock = _find_col(df, "STOCK")
    col_stock_min = _find_col(df, "STOCK MINIMO")
    col_porcentaje = _find_col(df, "PORCENTAJE COSTO") or _find_col(df, "PORCENTAJE")
    col_w_tienda1 = _find_col(df, "W-TIENDA1") or _find_col(df, "W TIENDA1")

    for c in df.columns:
        if df[c].dtype == "object":
            df[c] = df[c].apply(normalize_text_value)

    if col_nombre:
        df[col_nombre] = df[col_nombre].apply(clean_alnum_spaces)
    if col_desc:
        df[col_desc] = df[col_desc].apply(clean_alnum_spaces)

    if col_cat:
        df[col_cat] = df[col_cat].apply(clean_category_value)
        df[col_cat] = df[col_cat].apply(lambda x: x if str(x).strip() else "SIN CATEGORIA")
    else:
        df["CATEGORIA"] = "SIN CATEGORIA"
        col_cat = "CATEGORIA"

    if col_unidad:
        df[col_unidad] = df[col_unidad].apply(clean_unit_value)
    else:
        df["UNIDAD"] = "UNIDAD"
        col_unidad = "UNIDAD"

    if col_marca:
        df[col_marca] = df[col_marca].apply(lambda x: "S/M" if pd.isna(x) or str(x).strip() == "" else str(x).strip())
    else:
        df["MARCA"] = "S/M"
        col_marca = "MARCA"

    if col_modelo:
        df[col_modelo] = df[col_modelo].apply(lambda x: "S/M" if pd.isna(x) or str(x).strip() == "" else str(x).strip())
    else:
        df["MODELO"] = "S/M"
        col_modelo = "MODELO"

    # PORCENTAJE ahora SIEMPRE 18
    porcentaje_default = 18.0
    if col_porcentaje:
        df[col_porcentaje] = df[col_porcentaje].apply(to_number).apply(lambda x: porcentaje_default if _is_null(x) or x <= 0 else x)
    else:
        df["PORCENTAJE COSTO"] = porcentaje_default
        col_porcentaje = "PORCENTAJE COSTO"

    # Códigos (CONVERSIÓN)
    existing_codigo: set[str] = set()
    existing_barra: set[str] = set()
    existing_padre: set[str] = set()
    codes_fixed = 0

    def fix_code_generate(v, existing: set[str], prefix: str):
        nonlocal codes_fixed
        c = clean_product_code(v)
        if is_valid_product_code(c) and c not in existing:
            existing.add(c)
            return c
        codes_fixed += 1
        return generate_unique_code(existing, prefix=prefix)

    def fix_code_blank(v, existing: set[str]) -> str:
        c = clean_product_code(v)
        if is_valid_product_code(c) and c not in existing:
            existing.add(c)
            return c
        return ""

    if col_codigo:
        df[col_codigo] = df[col_codigo].apply(lambda v: fix_code_generate(v, existing_codigo, "CM"))
    if col_codigo_barra:
        df[col_codigo_barra] = df[col_codigo_barra].apply(lambda v: fix_code_blank(v, existing_barra))
    if col_codigo_padre:
        df[col_codigo_padre] = df[col_codigo_padre].apply(lambda v: fix_code_blank(v, existing_padre))

    # Numéricos + reglas
    def _is_blank_raw(v) -> bool:
        return v is None or (isinstance(v, float) and pd.isna(v)) or str(v).strip() == ""

    pv_was_blank = df[col_pventa].apply(_is_blank_raw) if col_pventa else None
    pc_was_blank = df[col_pcost].apply(_is_blank_raw) if col_pcost else None

    if col_stock:
        df[col_stock] = df[col_stock].apply(to_number).apply(lambda x: 0.0 if _is_null(x) else x)
        df.loc[df[col_stock] < 0, col_stock] = 0.0

    if col_stock_min:
        df[col_stock_min] = df[col_stock_min].apply(to_number)

    if col_pcost:
        df[col_pcost] = df[col_pcost].apply(to_number)
        if pc_was_blank is not None:
            df.loc[pc_was_blank, col_pcost] = 0.0
        df.loc[df[col_pcost].isna(), col_pcost] = 0.0
        df.loc[df[col_pcost] < 0, col_pcost] = 0.0

    if col_pventa:
        df[col_pventa] = df[col_pventa].apply(to_number)
        if pv_was_blank is not None:
            df.loc[pv_was_blank, col_pventa] = 1.0
        df.loc[df[col_pventa].isna(), col_pventa] = 1.0
        df.loc[df[col_pventa] < 1, col_pventa] = 1.0

    # pv > pc SOLO si la venta NO estaba vacía
    if col_pcost and col_pventa and (pv_was_blank is not None):
        must_fix = (~pv_was_blank) & (df[col_pventa] <= df[col_pcost])
        df.loc[must_fix, col_pventa] = df.loc[must_fix, col_pcost] + 1.0

    # IGV (SIEMPRE se aplica si los toggles están activos)
    if apply_igv_cost and col_pcost:
        df[col_pcost] = df[col_pcost].apply(lambda x: x * IGV_FACTOR if not _is_null(x) else x)
    if apply_igv_sale and col_pventa:
        df[col_pventa] = df[col_pventa].apply(lambda x: x * IGV_FACTOR if not _is_null(x) else x)

    # W-TIENDA1 = STOCK limpio con nombre dinámico
    nombre_columna_tienda = f"W-{tienda_nombre}"
    if col_stock:
        df[nombre_columna_tienda] = df[col_stock]
    
    # Eliminar la columna antigua si existe
    if col_w_tienda1 and col_w_tienda1 in df.columns:
        df.drop(columns=[col_w_tienda1], inplace=True)

    if round_numeric is not None:
        num_cols = df.select_dtypes(include=["number"]).columns
        df[num_cols] = df[num_cols].round(round_numeric)

    stats = {"codes_fixed": int(codes_fixed)}
    return df, stats


# ============================================================
# CONVERSIÓN: GENERAR QA MULTI-HOJA
# ============================================================
def build_conversion_qa_excel_bytes(
    df_input: pd.DataFrame,
    apply_igv_cost: bool = False,
    apply_igv_sale: bool = False,
    round_numeric: Optional[int] = None,
    tienda_nombre: str = "Tienda1",  # 👈 NUEVO PARÁMETRO
) -> tuple[bytes, dict]:
    ROW_ID_COL = ROW_ID_COL_DEFAULT

    df0 = df_input.copy()
    if ROW_ID_COL not in df0.columns:
        df0[ROW_ID_COL] = range(5, 5 + len(df0))

    cleaned, stats_clean = clean_conversion_output_df(
        df_final=df0,
        apply_igv_cost=apply_igv_cost,
        apply_igv_sale=apply_igv_sale,
        round_numeric=round_numeric,
        tienda_nombre=tienda_nombre,
    )

    col_codigo = None
    for c in cleaned.columns:
        if normalize_text_value(c) == "CODIGO":
            col_codigo = c
            break
    if col_codigo is None:
        col_codigo = _find_col(cleaned, "CODIGO")

    col_nombre = _find_col(cleaned, "NOMBRE")
    col_pcost = _find_col(cleaned, "PRECIO COSTO")
    col_pventa = _find_col(cleaned, "PRECIO VENTA")
    col_stock = _find_col(cleaned, "STOCK")
    col_cat = _find_col(cleaned, "CATEGORIA")
    col_unidad = _find_col(cleaned, "UNIDAD")

    errores = []
    ok_mask = []
    corrected = cleaned.copy()

    def push_error(i, codigo, colname, valor, err, solucion, comentario):
        errores.append(
            {
                "Código": codigo,
                "Ubicación (Fila / Columna)": f"{i+2} / {colname}",
                "Valor Detectado con error": valor,
                "Errores Detectados": err,
                "Solución Sugerida (Dato Listo)": solucion,
                "Comentarios": comentario,
            }
        )

    for i in range(len(cleaned)):
        ok = True

        codigo = cleaned.at[i, col_codigo] if col_codigo else ""
        nombre = cleaned.at[i, col_nombre] if col_nombre else ""
        unidad = cleaned.at[i, col_unidad] if col_unidad else "UNIDAD"
        categoria = cleaned.at[i, col_cat] if col_cat else "SIN CATEGORIA"

        pc = float(cleaned.at[i, col_pcost]) if col_pcost else 0.0
        pv = float(cleaned.at[i, col_pventa]) if col_pventa else 1.0
        st = float(cleaned.at[i, col_stock]) if col_stock else 0.0

        if not str(codigo).strip():
            ok = False
            push_error(i, codigo, col_codigo or "CODIGO", "", "CÓDIGO VACÍO", "", "Código es obligatorio.")

        if not str(nombre).strip():
            ok = False
            push_error(i, codigo, col_nombre or "NOMBRE", "", "NOMBRE VACÍO", "", "Nombre es obligatorio.")

        if not str(unidad).strip():
            ok = False
            push_error(i, codigo, col_unidad or "UNIDAD", "", "UNIDAD VACÍA", "UNIDAD", "Unidad es obligatoria.")

        if str(categoria).strip() == "SIN CATEGORIA":
            push_error(i, codigo, col_cat or "CATEGORIA", "", "CATEGORÍA VACÍA -> DEFAULT", "SIN CATEGORIA", "Se asignó default por categoría vacía/ inválida.")

        if col_stock and st < 0:
            ok = False
            corrected.at[i, col_stock] = 0.0
            push_error(i, codigo, col_stock, st, "STOCK NEGATIVO", 0.0, "Stock no puede ser negativo. Se ajustó a 0.")

        if col_pcost and pc < 0:
            ok = False
            corrected.at[i, col_pcost] = 0.0
            push_error(i, codigo, col_pcost, pc, "PRECIO COSTO < 0", 0.0, "Costo mínimo 0. Se ajustó a 0.")

        if col_pventa and pv < 1:
            ok = False
            corrected.at[i, col_pventa] = 1.0
            push_error(i, codigo, col_pventa, pv, "PRECIO VENTA < 1", 1.0, "Venta mínima 1. Se ajustó a 1.")

        # validación
        if col_pcost and col_pventa and pv <= pc:
            ok = False
            push_error(i, codigo, col_pventa, pv, "PRECIO VENTA <= PRECIO COSTO", pv, "Regla: venta > costo.")

        ok_mask.append(ok)

    errores_df = pd.DataFrame(
        errores,
        columns=[
            "Código",
            "Ubicación (Fila / Columna)",
            "Valor Detectado con error",
            "Errores Detectados",
            "Solución Sugerida (Dato Listo)",
            "Comentarios",
        ],
    )

    productos_ok = cleaned[pd.Series(ok_mask)].copy()
    productos_corregidos = corrected[~pd.Series(ok_mask)].copy()

    for dfx in (cleaned, corrected, productos_ok, productos_corregidos):
        if ROW_ID_COL in dfx.columns:
            dfx.drop(columns=[ROW_ID_COL], inplace=True)

    plantilla_api = cleaned.copy()
    if ROW_ID_COL in plantilla_api.columns:
        plantilla_api.drop(columns=[ROW_ID_COL], inplace=True)

    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        errores_df.to_excel(w, index=False, sheet_name="Errores_Detectados")
        productos_ok.to_excel(w, index=False, sheet_name="Productos_OK")
        productos_corregidos.to_excel(w, index=False, sheet_name="Productos_Corregidos")
        plantilla_api.to_excel(w, index=False, sheet_name="productos")

    stats = {
        "rows_before": int(len(df_input)),
        "rows_ok": int(len(productos_ok)),
        "rows_corrected": int(len(productos_corregidos)),
        "errors_count": int(len(errores_df)),
        "codes_fixed": int(stats_clean.get("codes_fixed", 0)),
    }
    return out.getvalue(), stats