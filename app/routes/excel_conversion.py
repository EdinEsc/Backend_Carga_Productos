from fastapi import APIRouter, UploadFile, File, BackgroundTasks, HTTPException, Query
from fastapi.responses import StreamingResponse
import uuid
import os
import re
import io

from app.services.conversion_processor import (
    generar_excel_conversion_bytes,
    leer_excel_conversion,
    ROW_ID_COL
)

router = APIRouter(prefix="/conversion", tags=["Conversion Excel"])

def cleanup_files(*paths: str):
    for p in paths:
        try:
            if p and os.path.exists(p):
                os.remove(p)
        except Exception:
            pass

def _parse_selected_row_ids_csv(selected_row_ids: str | None) -> set[int]:
    if not selected_row_ids:
        return set()
    parts = [p.strip() for p in selected_row_ids.split(",")]
    out = set()
    for p in parts:
        if not p:
            continue
        if not re.fullmatch(r"\d+", p):
            raise ValueError(f"ID inválido: {p}")
        out.add(int(p))
    return out

@router.post("/excel")
async def convertir_excel(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    apply_igv_cost: bool = Query(default=True, description="Aplicar IGV a precio de costo"),
    apply_igv_sale: bool = Query(default=True, description="Aplicar IGV a precio de venta"),
    is_selva: bool = Query(default=False, description="Modo selva (exonerado de IGV)"),
    tienda_nombre: str = Query(default="Tienda1", description="Nombre de la tienda para columna W-TIENDA1"),
    selected_row_ids: str | None = Query(default=None, description="CSV de __ROW_ID__: ej 5,9,12"),
):
    input_name = f"input_conv_{uuid.uuid4()}.xlsx"
    
    try:
        with open(input_name, "wb") as f:
            f.write(await file.read())
        
        selected_set = _parse_selected_row_ids_csv(selected_row_ids) if selected_row_ids else set()
        
        excel_bytes, stats = generar_excel_conversion_bytes(
            input_path=input_name,
            selected_row_ids=selected_set,
            apply_igv_cost=apply_igv_cost,
            apply_igv_sale=apply_igv_sale,
            is_selva=is_selva,
            tienda_nombre=tienda_nombre,
        )
        
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
            io.BytesIO(excel_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )
        
    except Exception as e:
        cleanup_files(input_name)
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/analyze")
async def analyze_conversion_excel(
    file: UploadFile = File(...),
):
    input_name = f"input_conv_{uuid.uuid4()}.xlsx"
    try:
        with open(input_name, "wb") as f:
            f.write(await file.read())
        
        df = leer_excel_conversion(input_name)
        
        grupos = []
        if "NOMBRE DEL PRODUCTO" in df.columns:
            s = df["NOMBRE DEL PRODUCTO"].astype(str).str.strip()
            dup_mask = s.ne("") & df["NOMBRE DEL PRODUCTO"].duplicated(keep=False)
            dups = df.loc[dup_mask]
            
            for nombre, grupo in dups.groupby("NOMBRE DEL PRODUCTO"):
                rows = []
                for _, row in grupo.iterrows():
                    row_dict = {}
                    for col in df.columns[:10]:
                        row_dict[col] = str(row[col])[:50]
                    row_dict[ROW_ID_COL] = int(row[ROW_ID_COL])
                    rows.append(row_dict)
                
                grupos.append({
                    "key": str(nombre),
                    "count": len(grupo),
                    "rows": rows
                })
        
        return {
            "has_duplicates": len(grupos) > 0,
            "groups": grupos,
            "columns_hint": list(df.columns[:20])
        }
        
    finally:
        cleanup_files(input_name)