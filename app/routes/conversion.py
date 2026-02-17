from fastapi import APIRouter, UploadFile, File
from fastapi.responses import FileResponse
import pandas as pd
import uuid
import os

router = APIRouter(prefix="/conversion", tags=["Conversion Excel"])

@router.post("/excel")
async def convertir_excel(file: UploadFile = File(...)):
    input_name = f"input_{uuid.uuid4()}.xlsx"
    output_name = f"output_{uuid.uuid4()}.xlsx"

    with open(input_name, "wb") as f:
        f.write(await file.read())

    df = pd.read_excel(input_name, header=None)

    # Fila de encabezados grandes (fila 3 → index 2)
    fila_headers = df.iloc[2]

    excluir = ["PRECIO LISTA 2", "PRECIO LISTA 3"]

    columnas_conversion = {}
    precio_lista_2 = None
    precio_lista_3 = None

    for idx, nombre in fila_headers.items():
        if pd.notna(nombre):
            nombre_str = str(nombre).strip().upper()

            if "PRECIO LISTA 2" in nombre_str:
                precio_lista_2 = idx
            elif "PRECIO LISTA 3" in nombre_str:
                precio_lista_3 = idx
            else:
                columnas_conversion[idx] = nombre_str.replace(" ", "")

    conversiones = []

    # Datos desde fila 5 (index 4)
    for i in range(4, len(df)):
        fila = df.iloc[i]
        partes = []

        for col, nombre in columnas_conversion.items():
            valor = fila[col]
            if pd.notna(valor):
                partes.append(f"{nombre}-{nombre}-{valor}")

        conversiones.append("#".join(partes))

    df_final = pd.DataFrame({
        "conversion": conversiones,
        "precio_lista_2": df.iloc[4:, precio_lista_2].values if precio_lista_2 is not None else None,
        "precio_lista_3": df.iloc[4:, precio_lista_3].values if precio_lista_3 is not None else None,
    })

    df_final.to_excel(output_name, index=False)

    os.remove(input_name)

    return FileResponse(
        output_name,
        filename="resultado_conversion.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
