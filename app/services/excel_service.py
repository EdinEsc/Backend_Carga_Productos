# =========================
# app/services/excel_service.py
# (REEMPLAZA TODO el archivo por este contenido)
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

# =========================
# Normalización base (Ñ OK)
# =========================
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


# =========================
# Limpieza específica
# =========================
def clean_alnum_spaces(v) -> str:
    s = normalize_text_value(v)
    s = re.sub(r"[^A-Z0-9 ]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def clean_category_value(v) -> str:
    s = clean_alnum_spaces(v)
    return s if re.search(r"[A-Z0-9]", s) else ""


ALNUM = set(string.ascii_uppercase + string.digits)


def clean_product_code(v) -> str:
    return re.sub(r"[^A-Z0-9]+", "", normalize_text_value(v))


def is_valid_product_code(code: str) -> bool:
    return (
        bool(code)
        and 4 <= len(code) <= 15
        and any(c.isdigit() for c in code)  # evita nombres puros
        and all(c in ALNUM for c in code)
    )


def generate_unique_code(existing: set[str], prefix="CM") -> str:
    while True:
        c = prefix + "".join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(10))
        if c not in existing:
            existing.add(c)
            return c


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
    except:
        return None



def _find_col(df: pd.DataFrame, name: str) -> Optional[str]:
    name = normalize_text_value(name)
    for c in df.columns:
        if name in normalize_text_value(c):
            return c
    return None


def _is_null(x) -> bool:
    return x is None or (isinstance(x, float) and pd.isna(x))


# =========================
# JSON safe helper
# =========================
def _json_safe(v):
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if hasattr(v, "isoformat"):
        try:
            return v.isoformat()
        except:
            return str(v)
    return v


# =========================
# Duplicados por NOMBRE (para frontend)
# =========================
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


# =========================
# NORMALIZACIÓN A DF (para /analyze)
# =========================
def normalize_to_dataframe(
    excel_bytes: bytes,
    round_numeric: Optional[int] = None,
) -> tuple[pd.DataFrame, dict, dict]:
    df = pd.read_excel(io.BytesIO(excel_bytes), engine="openpyxl")
    before_rows = len(df)

    # Normaliza nombres de columnas
    df.columns = [normalize_text_value(c) for c in df.columns]

    # Detecta columnas
    col_codigo = _find_col(df, "CODIGO")
    col_nombre = _find_col(df, "NOMBRE")
    col_desc = _find_col(df, "DESCRIPCION")
    col_cat = _find_col(df, "CATEGORIA")
    col_pcost = _find_col(df, "PRECIO DE COSTO")
    col_pventa = _find_col(df, "PRECIO DE VENTA")
    col_unidad = _find_col(df, "UNIDAD")
    col_stock = _find_col(df, "CANTIDAD") or _find_col(df, "STOCK")
    col_stock_min = _find_col(df, "STOCK MINIMO")

    # Normaliza texto en object
    for c in df.columns:
        if df[c].dtype == "object":
            df[c] = df[c].apply(normalize_text_value)

    # Limpieza estricta
    if col_nombre:
        df[col_nombre] = df[col_nombre].apply(clean_alnum_spaces)
    if col_desc:
        df[col_desc] = df[col_desc].apply(clean_alnum_spaces)
    if col_cat:
        df[col_cat] = df[col_cat].apply(clean_category_value)

    # Código: corrige inválidos/duplicados
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
    }

    stats = {"rows_before": int(before_rows), "codes_fixed": int(codes_fixed)}
    return df, meta, stats


# =========================
# FUNCIÓN PRINCIPAL (genera Excel QA)
# =========================


# def normalize_excel_bytes(
#     excel_bytes: bytes,
#     round_numeric: Optional[int] = None,
#     selected_row_ids: Optional[list[int]] = None,
# ) -> Tuple[bytes, dict]:

def normalize_excel_bytes(
    excel_bytes: bytes,
    round_numeric: Optional[int] = None,
    selected_row_ids: Optional[list[int]] = None,
    apply_igv_cost: bool = False,
    apply_igv_sale: bool = False,
) -> Tuple[bytes, dict]:

    ROW_ID_COL = "__ROW_ID__"

    df = pd.read_excel(io.BytesIO(excel_bytes), engine="openpyxl")
    before_rows = len(df)

    # Normaliza nombres de columnas
    df.columns = [normalize_text_value(c) for c in df.columns]

    # Row id estable (crear DESPUÉS de normalizar columnas)
    df[ROW_ID_COL] = range(2, 2 + len(df))

    # Detecta columnas
    col_codigo = _find_col(df, "CODIGO")
    col_nombre = _find_col(df, "NOMBRE")
    col_desc = _find_col(df, "DESCRIPCION")
    col_cat = _find_col(df, "CATEGORIA")
    col_pcost = _find_col(df, "PRECIO DE COSTO")
    col_pventa = _find_col(df, "PRECIO DE VENTA")
    col_unidad = _find_col(df, "UNIDAD")
    col_stock = _find_col(df, "CANTIDAD") or _find_col(df, "STOCK")
    col_stock_min = _find_col(df, "STOCK MINIMO")

    # Normaliza texto en object
    for c in df.columns:
        if df[c].dtype == "object":
            df[c] = df[c].apply(normalize_text_value)

    # Limpieza estricta
    if col_nombre:
        df[col_nombre] = df[col_nombre].apply(clean_alnum_spaces)
    if col_desc:
        df[col_desc] = df[col_desc].apply(clean_alnum_spaces)
    if col_cat:
        df[col_cat] = df[col_cat].apply(clean_category_value)

    # Si el frontend envía selección, filtrar a esas filas
    if selected_row_ids is not None and len(selected_row_ids) > 0 and col_nombre:
        wanted = set(int(x) for x in selected_row_ids)

        # filas que pertenecen a un nombre duplicado (en el DF ya normalizado)
        dup_mask = df[col_nombre].astype(str).str.strip().ne("") & df[col_nombre].duplicated(keep=False)
        dup_row_ids = set(df.loc[dup_mask, ROW_ID_COL].astype(int).tolist())

        # Regla:
        # - Si la fila NO es duplicada => se queda
        # - Si la fila ES duplicada => solo se queda si fue seleccionada
        keep_mask = (~df[ROW_ID_COL].isin(dup_row_ids)) | (df[ROW_ID_COL].isin(wanted))

        df = df.loc[keep_mask].copy()
        df = df.reset_index(drop=True)


    # Código: corrige inválidos/duplicados
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

    # Numéricos + defaults (ANTES de auditoría)
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

    # =========================
    # IGV (según toggles frontend)
    # =========================
    if apply_igv_cost and col_pcost:
        df[col_pcost] = df[col_pcost].apply(lambda x: x * IGV_FACTOR if not _is_null(x) else x)

    if apply_igv_sale and col_pventa:
        df[col_pventa] = df[col_pventa].apply(lambda x: x * IGV_FACTOR if not _is_null(x) else x)


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

    if round_numeric is not None:
        num_cols = df.select_dtypes(include=["number"]).columns
        df[num_cols] = df[num_cols].round(round_numeric)

    # =========================
    # Auditoría
    # =========================
    errores = []
    ok_mask = []
    corrected = df.copy()

    def push_error(i, codigo, colname, valor, err, solucion, comentario):
        errores.append({
            "Código": codigo,
            "Ubicación (Fila / Columna)": f"{i+2} / {colname}",
            "Valor Detectado con error": valor,
            "Errores Detectados": err,
            "Solución Sugerida (Dato Listo)": solucion,
            "Comentarios": comentario,
        })

    for i in range(len(df)):
        ok = True

        codigo = df.at[i, col_codigo] if col_codigo else ""
        nombre = df.at[i, col_nombre] if col_nombre else ""
        unidad = df.at[i, col_unidad] if col_unidad else ""
        categoria = df.at[i, col_cat] if col_cat else "SIN CATEGORIA"

        pc = float(df.at[i, col_pcost])
        pv = float(df.at[i, col_pventa])
        st = float(df.at[i, col_stock])

        if not str(codigo).strip():
            ok = False
            push_error(i, codigo, col_codigo or "CODIGO", "", "CÓDIGO VACÍO", codigo, "Código es obligatorio.")

        if not str(nombre).strip():
            ok = False
            push_error(i, codigo, col_nombre or "NOMBRE", "", "NOMBRE VACÍO", "", "Nombre es obligatorio.")

        if not str(unidad).strip():
            ok = False
            push_error(i, codigo, col_unidad or "UNIDAD", "", "UNIDAD VACÍA", "", "Unidad es obligatoria.")

        if str(categoria).strip() == "SIN CATEGORIA":
            push_error(i, codigo, col_cat or "CATEGORIA", "", "CATEGORÍA VACÍA -> DEFAULT",
                       "SIN CATEGORIA", "Se asignó default por categoría vacía/ inválida.")

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
            push_error(i, codigo, col_pventa, pv2, "PRECIO VENTA <= PRECIO COSTO", pv2,
                       "Regla: venta debe ser mayor que costo. No se ajusta automático.")

        ok_mask.append(ok)

    errores_df = pd.DataFrame(errores, columns=[
        "Código",
        "Ubicación (Fila / Columna)",
        "Valor Detectado con error",
        "Errores Detectados",
        "Solución Sugerida (Dato Listo)",
        "Comentarios",
    ])

    productos_ok = df[pd.Series(ok_mask)].copy()
    productos_corregidos = corrected[~pd.Series(ok_mask)].copy()

    # Eliminar ROW_ID_COL de exportaciones
    for dfx in (df, corrected, productos_ok, productos_corregidos):
        dfx.drop(columns=[ROW_ID_COL], errors="ignore", inplace=True)

    final_df = pd.concat([productos_ok, productos_corregidos], ignore_index=True)

    plantilla_api = pd.DataFrame({
        "Nombre": final_df[col_nombre] if col_nombre else "",
        "Descripcion": final_df[col_desc] if col_desc else "",
        "codigo padre": "",
        "codigo": final_df[col_codigo] if col_codigo else "",
        "Codigo alterno": "",
        "codigo barra": final_df[col_codigo] if col_codigo else "",
        "Categoria": final_df[col_cat],
        "stock": final_df[col_stock],
        "stock minimo": final_df[col_stock_min] if col_stock_min else "",
        "precio costo": final_df[col_pcost],
        "precio venta": final_df[col_pventa],
        "porcentaje costo": 18,
        "R-Lista1": "0-0-0",
        "unidad": final_df[col_unidad] if col_unidad else "",
        "Marca": "S/M",
        "Modelo": "S/M",
        "Almacenable": "SI",
        "W-Tienda1": final_df[col_stock],
    })

    plantilla_api = plantilla_api[[
        "Nombre", "Descripcion", "codigo padre", "codigo", "Codigo alterno", "codigo barra",
        "Categoria", "stock", "stock minimo", "precio costo", "precio venta",
        "porcentaje costo", "R-Lista1", "unidad", "Marca", "Modelo", "Almacenable", "W-Tienda1"
    ]]

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
