import io
import pandas as pd
from typing import Optional, Tuple
from .excel_cleaners import (
    normalize_text_value, clean_alnum_spaces, clean_category_value,
    clean_unit_value, clean_product_code, is_valid_product_code,
    generate_unique_code, to_number, _find_col, _is_null, _drop_all_empty_rows,
    IGV_FACTOR, ROW_ID_COL_DEFAULT
)

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
# CARGA POR CONVERSIÓN: Limpieza del output
# ============================================================
def clean_conversion_output_df(
    df_final: pd.DataFrame,
    apply_igv_cost: bool = False,
    apply_igv_sale: bool = False,
    round_numeric: Optional[int] = None,
    tienda_nombre: str = "Tienda1",  
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
    tienda_nombre: str = "Tienda1",  
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