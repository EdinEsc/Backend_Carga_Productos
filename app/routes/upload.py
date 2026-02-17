from uuid import uuid4
import io

from fastapi import APIRouter, File, UploadFile, Query, HTTPException, Body
from fastapi.responses import StreamingResponse

from app.services.excel_service import (
    normalize_excel_bytes,
    normalize_to_dataframe,
    build_duplicate_groups,
)

router = APIRouter(prefix="/excel", tags=["excel"])

UPLOADS: dict[str, bytes] = {}


@router.post("/analyze")
async def analyze_excel(
    file: UploadFile = File(...),
    round_numeric: int | None = Query(default=None, description="Ej: 2 para redondear a 2 decimales"),
):
    content = await file.read()

    # Normaliza primero (para que los duplicados se muestren ya con todos los campos limpios)
    df_norm, meta, _stats = normalize_to_dataframe(content, round_numeric=round_numeric)

    col_nombre = meta.get("col_nombre")
    if not col_nombre:
        raise HTTPException(status_code=400, detail="No se encontró columna NOMBRE")

    ROW_ID_COL = "__ROW_ID__"
    df_norm[ROW_ID_COL] = range(2, 2 + len(df_norm))

    groups = build_duplicate_groups(df_norm, col_nombre)

    upload_id = str(uuid4())
    UPLOADS[upload_id] = content

    return {
        "upload_id": upload_id,
        "has_duplicates": len(groups) > 0,
        "groups": groups,
        "columns_hint": list(df_norm.columns),
    }


@router.post("/normalize")
async def normalize_excel(
    upload_id: str = Query(...),

    # ⬇️ BOTÓN 1 y BOTÓN 2
    apply_igv_cost: bool = Query(default=False, description="Aplicar IGV a precio de costo"),
    apply_igv_sale: bool = Query(default=False, description="Aplicar IGV a precio de venta"),
    
    # ⬇️ NUEVO: Parámetro para Selva
    is_selva: bool = Query(default=False, description="Activar modo Selva (0% y sin IGV)"),

    selected_row_ids: list[int] = Body(default=[]),
    round_numeric: int | None = Query(default=None, description="Ej: 2 para redondear a 2 decimales"),
):
    if upload_id not in UPLOADS:
        raise HTTPException(status_code=400, detail="upload_id inválido o expirado")

    content = UPLOADS[upload_id]

    # Pasar TODOS los parámetros al servicio
    cleaned_bytes, stats = normalize_excel_bytes(
        excel_bytes=content,
        round_numeric=round_numeric,
        selected_row_ids=selected_row_ids,
        apply_igv_cost=apply_igv_cost,
        apply_igv_sale=apply_igv_sale,
        is_selva=is_selva,  # ⬅️ IMPORTANTE: pasar el parámetro
    )

    filename = "archivo_QA.xlsx"

    headers = {
        "X-Rows-Before": str(stats.get("rows_before", "")),
        "X-Rows-OK": str(stats.get("rows_ok", "")),
        "X-Rows-Corrected": str(stats.get("rows_corrected", "")),
        "X-Errors-Count": str(stats.get("errors_count", "")),
        "X-Codes-Fixed": str(stats.get("codes_fixed", stats.get("codes_fixed_or_regenerated", ""))),
    }

    return StreamingResponse(
        io.BytesIO(cleaned_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={**headers, "Content-Disposition": f'attachment; filename="{filename}"'},
    )