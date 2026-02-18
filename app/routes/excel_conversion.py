from fastapi import APIRouter, UploadFile, File
from fastapi.responses import FileResponse
import pandas as pd
import uuid
import os

router = APIRouter(prefix="/conversion", tags=["Conversion Excel"])


@router.post("/excel")
async def convertir_excel(file: UploadFile = File(...)):
    # =========================
    # Archivos temporales
    # =========================
    input_name = f"input_{uuid.uuid4()}.xlsx"
    output_name = f"output_{uuid.uuid4()}.xlsx"

    with open(input_name, "wb") as f:
        f.write(await file.read())

    # =========================
    # Leer Excel SIN headers
    # =========================
    df = pd.read_excel(input_name, header=None)

    # =========================
    # Configuración fija
    # =========================
    header_row = 2      # fila 3 en Excel
    data_start = 4      # fila 5 en Excel

    # =========================
    # Columnas que NO entran en conversion
    # =========================
    excluir = [
        "CODIGO DEL PRODUCTO","CODIGO ALTERNO","NOMBRE DEL PRODUCTO","DESCRIPCION","CATEGORIA","TIPO",
        "PRECIO DE COSTO","PRECIO DE VENTA PRINCIPAL","RANGO DEL PRECIO","MODELO","UNIDAD","STOCK","MARCA",
        "PRECIO LISTA 2",
        "PRECIO LISTA 3"
    ]

    # =========================
    # Columnas que SÍ deben salir en el Excel final
    # =========================
    columnas_salida = [
        "CODIGO DEL PRODUCTO","CODIGO ALTERNO","NOMBRE DEL PRODUCTO","DESCRIPCION","CATEGORIA","TIPO",
        "PRECIO DE COSTO","PRECIO DE VENTA PRINCIPAL","RANGO DEL PRECIO","MODELO","UNIDAD","STOCK","MARCA",
        "PRECIO LISTA 2",
        "PRECIO LISTA 3"
    ]

    # =========================
    # Leer encabezados
    # =========================
    fila_headers = df.iloc[header_row]

    columnas_todas = {}        # todas las columnas detectadas
    columnas_conversion = {}   # solo para armar "conversion"

    for idx, nombre in fila_headers.items():
        if pd.notna(nombre):
            nombre_str = str(nombre).strip().upper()
            columnas_todas[nombre_str] = idx

            if nombre_str not in excluir:
                columnas_conversion[idx] = nombre_str.replace(" ", "")

    # =========================
    # Construir conversion
    # =========================
    conversiones = []

    for i in range(data_start, len(df)):
        fila = df.iloc[i]
        partes = []

        for col, nombre in columnas_conversion.items():
            valor = fila[col]
            if pd.notna(valor):
                partes.append(f"{nombre}-{nombre}-{valor}")

        conversiones.append("#".join(partes))

    # =========================
    # Construir Excel FINAL
    # =========================
    df_final = pd.DataFrame()

    # 👉 SOLO las columnas permitidas
    for nombre in columnas_salida:
        if nombre in columnas_todas:
            idx = columnas_todas[nombre]
            df_final[nombre] = df.iloc[data_start:, idx].values

    # 👉 agregar conversion
    df_final["conversion"] = conversiones

    # =========================
    # Guardar Excel
    # =========================
    df_final.to_excel(output_name, index=False)

    os.remove(input_name)

    return FileResponse(
        output_name,
        filename="resultado_conversion.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )