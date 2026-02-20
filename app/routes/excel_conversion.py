# # =========================
# # excel_conversion.py (COMPLETO - LISTO PARA COPIAR Y PEGAR)
# # =========================
# from fastapi import APIRouter, UploadFile, File, BackgroundTasks, Query, HTTPException, Body
# from fastapi.responses import StreamingResponse
# import pandas as pd
# import uuid
# import os
# import io
# import re
# import unicodedata

# from app.services.excel_service import (
#     build_conversion_df_from_file,
#     build_conversion_qa_excel_bytes,
#     build_duplicate_groups_with_row_id,
# )

# router = APIRouter(prefix="/conversion", tags=["Conversion Excel"])

# ROW_ID_COL = "__ROW_ID__"


# def cleanup_files(*paths: str):
#     for p in paths:
#         try:
#             if p and os.path.exists(p):
#                 os.remove(p)
#         except Exception:
#             pass


# def _find_col_loose(df: pd.DataFrame, target: str) -> str | None:
#     """Encuentra columna por contains (case/accent insensitive)."""
#     def norm(s: str) -> str:
#         s = str(s).strip()
#         s = unicodedata.normalize("NFKD", s)
#         s = "".join(c for c in s if not unicodedata.combining(c))
#         s = s.upper()
#         s = re.sub(r"\s+", " ", s).strip()
#         return s

#     t = norm(target)
#     for c in df.columns:
#         if t in norm(c):
#             return c
#     return None

 
# def _filter_duplicates_by_selected(df: pd.DataFrame, col_nombre: str, selected_ids: set[int]) -> pd.DataFrame:
#     """
#     Igual que en carga normal:
#     - Si un nombre está duplicado, solo se queda la(s) fila(s) seleccionada(s).
#     - Lo NO duplicado se queda siempre.
#     """
#     if not col_nombre or col_nombre not in df.columns:
#         return df

#     s = df[col_nombre].astype(str).str.strip()
#     dup_mask = s.ne("") & df[col_nombre].duplicated(keep=False)

#     if not dup_mask.any():
#         return df

#     dup_row_ids = set(df.loc[dup_mask, ROW_ID_COL].astype(int).tolist())

#     keep_mask = (~df[ROW_ID_COL].isin(dup_row_ids)) | (df[ROW_ID_COL].isin(selected_ids))
#     return df.loc[keep_mask].copy().reset_index(drop=True)


# # =========================
# # 1) ANALYZE (como /excel/analyze)
# # =========================
# @router.post("/analyze")
# async def analyze_conversion_excel(
#     file: UploadFile = File(...),
# ):
#     input_name = f"input_conv_{uuid.uuid4()}.xlsx"
#     try:
#         with open(input_name, "wb") as f:
#             f.write(await file.read())

#         # arma el df_final de conversión con __ROW_ID__
#         df_final = build_conversion_df_from_file(input_name)

#         # detectar duplicados por Nombre (usa __ROW_ID__ en rows)
#         col_nombre = _find_col_loose(df_final, "NOMBRE")
#         if not col_nombre:
#             raise HTTPException(status_code=400, detail="No se encontró columna Nombre en conversión")

#         groups = build_duplicate_groups_with_row_id(df_final, col_nombre, row_id_col=ROW_ID_COL)

#         return {
#             "has_duplicates": len(groups) > 0,
#             "groups": groups,
#             "columns_hint": list(df_final.columns),
#         }
#     finally:
#         cleanup_files(input_name)


# # =========================
# # 2) EXCEL (descarga QA multi-hoja)
# #    - permite filtrar duplicados con selected_row_ids (lista en JSON body)
# # =========================
# @router.post("/excel")
# async def convertir_excel(
#     background_tasks: BackgroundTasks,
#     file: UploadFile = File(...),

#     # mismos flags que carga normal
#     apply_igv_cost: bool = Query(default=True, description="Aplicar IGV a precio de costo"),
#     apply_igv_sale: bool = Query(default=True, description="Aplicar IGV a precio de venta"),
#     is_selva: bool = Query(default=False, description="Modo selva (exonerado de IGV)"),
#     round_numeric: int | None = Query(default=None, description="Ej: 2 para redondear a 2 decimales"),

#     # ✅ seleccionados por duplicados: lista JSON (igual que /excel/normalize)
#     selected_row_ids: list[int] = Body(default=[]),
# ):
#     input_name = f"input_conv_{uuid.uuid4()}.xlsx"

#     try:
#         with open(input_name, "wb") as f:
#             f.write(await file.read())

#         # arma df_final con __ROW_ID__
#         df_final = build_conversion_df_from_file(input_name)

#         # filtrar duplicados si el frontend mandó selección
#         col_nombre = _find_col_loose(df_final, "NOMBRE")
#         if col_nombre and selected_row_ids:
#             try:
#                 selected_set = set(int(x) for x in selected_row_ids)
#             except Exception:
#                 raise HTTPException(status_code=400, detail="selected_row_ids inválido (lista de enteros)")

#             df_final = _filter_duplicates_by_selected(df_final, col_nombre, selected_set)

#         # generar QA multi-hoja
#         qa_bytes, stats = build_conversion_qa_excel_bytes(
#             df_input=df_final,
#             is_selva=is_selva,
#             apply_igv_cost=apply_igv_cost,
#             apply_igv_sale=apply_igv_sale,
#             round_numeric=round_numeric,
#         )

#         # cleanup input en background (opcional, pero consistente con tu patrón)
#         background_tasks.add_task(cleanup_files, input_name)

#         headers = {
#             "X-Rows-Before": str(stats.get("rows_before", "")),
#             "X-Rows-OK": str(stats.get("rows_ok", "")),
#             "X-Rows-Corrected": str(stats.get("rows_corrected", "")),
#             "X-Errors-Count": str(stats.get("errors_count", "")),
#             "X-Codes-Fixed": str(stats.get("codes_fixed", "")),
#             "Content-Disposition": 'attachment; filename="resultado_conversion_QA.xlsx"',
#         }

#         return StreamingResponse(
#             io.BytesIO(qa_bytes),
#             media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
#             headers=headers,
#         )
#     finally:
#         # si uvicorn corta antes del background task, igual limpiamos
#         cleanup_files(input_name)



# =========================
# excel_conversion.py (COMPLETO - LISTO PARA COPIAR Y PEGAR)
# =========================
from fastapi import APIRouter, UploadFile, File, BackgroundTasks, Query, HTTPException
from fastapi.responses import StreamingResponse
import pandas as pd
import uuid
import os
import io
import re
import unicodedata

from app.services.excel_service import (
    build_conversion_df_from_file,
    build_conversion_qa_excel_bytes,
    build_duplicate_groups_with_row_id,
)

router = APIRouter(prefix="/conversion", tags=["Conversion Excel"])

ROW_ID_COL = "__ROW_ID__"


def cleanup_files(*paths: str):
    for p in paths:
        try:
            if p and os.path.exists(p):
                os.remove(p)
        except Exception:
            pass


def _find_col_loose(df: pd.DataFrame, target: str) -> str | None:
    """Encuentra columna por contains (case/accent insensitive)."""
    def norm(s: str) -> str:
        s = str(s).strip()
        s = unicodedata.normalize("NFKD", s)
        s = "".join(c for c in s if not unicodedata.combining(c))
        s = s.upper()
        s = re.sub(r"\s+", " ", s).strip()
        return s

    t = norm(target)
    for c in df.columns:
        if t in norm(c):
            return c
    return None


def _parse_selected_row_ids_csv(selected_row_ids: str | None) -> set[int]:
    """
    selected_row_ids viene como CSV en query: "5,9,12"
    Retorna set[int]. Si viene vacío/None -> set()
    """
    if not selected_row_ids:
        return set()

    parts = [p.strip() for p in selected_row_ids.split(",")]
    out: set[int] = set()
    for p in parts:
        if not p:
            continue
        if not re.fullmatch(r"\d+", p):
            raise ValueError(f"ID inválido: {p}")
        out.add(int(p))
    return out


def _filter_duplicates_by_selected(df: pd.DataFrame, col_nombre: str, selected_ids: set[int]) -> pd.DataFrame:
    """
    Igual que en carga normal:
    - Si un nombre está duplicado, solo se queda la(s) fila(s) seleccionada(s).
    - Lo NO duplicado se queda siempre.
    """
    if not col_nombre or col_nombre not in df.columns:
        return df

    s = df[col_nombre].astype(str).str.strip()
    dup_mask = s.ne("") & df[col_nombre].duplicated(keep=False)

    if not dup_mask.any():
        return df

    dup_row_ids = set(df.loc[dup_mask, ROW_ID_COL].astype(int).tolist())
    keep_mask = (~df[ROW_ID_COL].isin(dup_row_ids)) | (df[ROW_ID_COL].isin(selected_ids))
    return df.loc[keep_mask].copy().reset_index(drop=True)


# =========================
# 1) ANALYZE (como /excel/analyze)
# =========================
@router.post("/analyze")
async def analyze_conversion_excel(
    file: UploadFile = File(...),
):
    input_name = f"input_conv_{uuid.uuid4()}.xlsx"
    try:
        with open(input_name, "wb") as f:
            f.write(await file.read())

        # arma el df_final de conversión con __ROW_ID__
        df_final = build_conversion_df_from_file(input_name)

        # detectar duplicados por Nombre (usa __ROW_ID__ en rows)
        col_nombre = _find_col_loose(df_final, "NOMBRE")
        if not col_nombre:
            raise HTTPException(status_code=400, detail="No se encontró columna Nombre en conversión")

        groups = build_duplicate_groups_with_row_id(df_final, col_nombre, row_id_col=ROW_ID_COL)

        return {
            "has_duplicates": len(groups) > 0,
            "groups": groups,
            "columns_hint": list(df_final.columns),
        }
    finally:
        cleanup_files(input_name)


# =========================
# 2) EXCEL (descarga QA multi-hoja)
#    - selected_row_ids por QUERY CSV (porque es multipart/form-data)
#      Ej: /conversion/excel?selected_row_ids=5,9,12
# =========================
@router.post("/excel")
async def convertir_excel(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),

    # mismos flags que carga normal
    apply_igv_cost: bool = Query(default=True, description="Aplicar IGV a precio de costo"),
    apply_igv_sale: bool = Query(default=True, description="Aplicar IGV a precio de venta"),
    is_selva: bool = Query(default=False, description="Modo selva (exonerado de IGV)"),
    round_numeric: int | None = Query(default=None, description="Ej: 2 para redondear a 2 decimales"),

    # ✅ seleccionados por duplicados (CSV)
    selected_row_ids: str | None = Query(default=None, description="CSV de __ROW_ID__: ej 5,9,12"),
):
    input_name = f"input_conv_{uuid.uuid4()}.xlsx"

    try:
        with open(input_name, "wb") as f:
            f.write(await file.read())

        # arma df_final con __ROW_ID__
        df_final = build_conversion_df_from_file(input_name)

        # filtrar duplicados si el frontend mandó selección
        col_nombre = _find_col_loose(df_final, "NOMBRE")
        if col_nombre:
            try:
                selected_set = _parse_selected_row_ids_csv(selected_row_ids)
            except Exception:
                raise HTTPException(status_code=400, detail="selected_row_ids inválido (usa CSV: 5,9,12)")

            if selected_set:
                df_final = _filter_duplicates_by_selected(df_final, col_nombre, selected_set)

        # generar QA multi-hoja
        qa_bytes, stats = build_conversion_qa_excel_bytes(
            df_input=df_final,
            is_selva=is_selva,
            apply_igv_cost=apply_igv_cost,
            apply_igv_sale=apply_igv_sale,
            round_numeric=round_numeric,
        )

        # cleanup input en background
        background_tasks.add_task(cleanup_files, input_name)

        headers = {
            "X-Rows-Before": str(stats.get("rows_before", "")),
            "X-Rows-OK": str(stats.get("rows_ok", "")),
            "X-Rows-Corrected": str(stats.get("rows_corrected", "")),
            "X-Errors-Count": str(stats.get("errors_count", "")),
            "X-Codes-Fixed": str(stats.get("codes_fixed", "")),
            "Content-Disposition": 'attachment; filename="resultado_conversion_QA.xlsx"',
        }

        return StreamingResponse(
            io.BytesIO(qa_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )
    finally:
        # si uvicorn corta antes del background task, igual limpiamos
        cleanup_files(input_name)