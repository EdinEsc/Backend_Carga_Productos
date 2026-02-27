import io
import pandas as pd
from typing import Optional, Tuple
from .excel_cleaners import (
    normalize_text_value, clean_alnum_spaces, clean_category_value,
    clean_unit_value, clean_product_code, is_valid_product_code,
    generate_unique_code, to_number, _find_col, _is_null, _json_safe,
    process_product_code, IGV_FACTOR, ROW_ID_COL_DEFAULT
)

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
        resultado = process_product_code(v, existing)
        if resultado["es_generico"]:
            codes_fixed += 1
        return resultado["codigo_final"]

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

    # Para NOMBRE, solo convertir a mayúsculas sin limpieza de caracteres especiales
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

    # Códigos: NUEVA VERSIÓN CON REGISTRO DE ESTADO
    existing_codigo = set()
    codes_fixed = 0
    codigos_info = []  # Para tracking en frontend

    def procesar_codigo_con_registro(v, row_idx):
        nonlocal codes_fixed
        resultado = process_product_code(v, existing_codigo, row_idx)
        
        if resultado["es_generico"]:
            codes_fixed += 1
        
        # Guardar info para frontend/Excel
        codigos_info.append({
            "fila": row_idx + 5,  # +5 por el header
            "original": resultado["codigo_original"],
            "final": resultado["codigo_final"],
            "es_generico": resultado["es_generico"],
            "razon": resultado["razon"]
        })
        
        return resultado["codigo_final"]

    if col_codigo:
        df[col_codigo] = [procesar_codigo_con_registro(v, i) for i, v in enumerate(df[col_codigo])]

    def fix_code_blank_factory():
        seen = set()

        def _fix(v):
            c = clean_product_code(v)
            if is_valid_product_code(c) and c not in seen:
                seen.add(c)
                return c
            return ""

        return _fix


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

    # APLICAR IGV A TODOS LOS DATOS ANTES DE LA AUDITORÍA
    df_con_igv = df.copy()
    
    if apply_igv_cost and col_pcost:
        df_con_igv[col_pcost] = df_con_igv[col_pcost].apply(lambda x: x * IGV_FACTOR if not _is_null(x) else x)
    
    if apply_igv_sale and col_pventa:
        df_con_igv[col_pventa] = df_con_igv[col_pventa].apply(lambda x: x * IGV_FACTOR if not _is_null(x) else x)

    # 🔴 REDONDEAR AQUÍ DESPUÉS DE IGV Y ANTES DE AUDITORÍA 🔴
    if round_numeric is not None:
        num_cols = df_con_igv.select_dtypes(include=["number"]).columns
        df_con_igv[num_cols] = df_con_igv[num_cols].round(round_numeric)

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

    # DataFrame de códigos procesados
    codigos_df = pd.DataFrame(codigos_info, columns=[
        "Fila", "Código Original", "Código Final", 
        "Es Genérico", "Razón"
    ])

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

    # Usar el nombre de la tienda para la columna
    nombre_columna_tienda = f"W-{tienda_nombre}"

    plantilla_api = pd.DataFrame(
        {
            "Nombre": final_df[col_nombre] if col_nombre else "",
            "Descripcion": final_df[col_desc] if col_desc else "",
            "codigo padre": final_df[col_codigo_padre] if col_codigo_padre else codigo_padre_default,
            "codigo": final_df[col_codigo] if col_codigo else "",
            "Codigo alterno": final_df[col_codigo_alterno] if col_codigo_alterno else codigo_alterno_default,
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

    # Lista de columnas en orden exacto
    columnas_ordenadas = [
        "Nombre",
        "Descripcion",
        "codigo padre",
        "codigo",
        "Codigo alterno",
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
        # codigos_df.to_excel(w, index=False, sheet_name="Códigos_Procesados")
        productos_ok.to_excel(w, index=False, sheet_name="Productos_OK")
        productos_corregidos.to_excel(w, index=False, sheet_name="Productos_Corregidos")
        plantilla_api.to_excel(w, index=False, sheet_name="productos")

    stats = {
        "rows_before": int(before_rows),
        "rows_ok": int(len(productos_ok)),
        "rows_corrected": int(len(productos_corregidos)),
        "errors_count": int(len(errores_df)),
        "codes_fixed": int(codes_fixed),
        "codigos_info": codigos_info,  # Para frontend
    }

    return out.getvalue(), stats
