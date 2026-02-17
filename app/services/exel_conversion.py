import pandas as pd

# Leer Excel
df = pd.read_excel("conversion.xlsx", header=None)

# Fila donde están los encabezados grandes
fila_headers = df.iloc[2]

# Columnas a excluir de conversion
excluir = ["1","2","3","4","5","6","7","8","9","10""11","12","PRECIO LISTA 2", "PRECIO LISTA 3"]

columnas_conversion = {}

for idx, nombre in fila_headers.items():
    if pd.notna(nombre):
        nombre_str = str(nombre).strip().upper()
        if nombre_str not in excluir:
            columnas_conversion[idx] = nombre_str.replace(" ", "")

conversiones = []

# Recorremos filas con datos (desde fila 5)
for i in range(4, len(df)):
    fila = df.iloc[i]
    partes = []

    for col, nombre in columnas_conversion.items():
        valor = fila[col]
        if pd.notna(valor):
            partes.append(f"{nombre}-{nombre}-{valor}")

    conversiones.append("#".join(partes))

# Detectar columnas de precios
precio_lista_2 = None
precio_lista_3 = None

for idx, nombre in fila_headers.items():
    if pd.notna(nombre):
        n = str(nombre).upper()
        if "PRECIO LISTA 2" in n:
            precio_lista_2 = idx
        if "PRECIO LISTA 3" in n:
            precio_lista_3 = idx

# Crear DataFrame final
df_final = pd.DataFrame({
    "conversion": conversiones,
    "precio_lista_2": df.iloc[4:, precio_lista_2].values if precio_lista_2 is not None else None,
    "precio_lista_3": df.iloc[4:, precio_lista_3].values if precio_lista_3 is not None else None
})

# Exportar
df_final.to_excel("resultado_conversion.xlsx", index=False)

print("✅ Listo. Se generó resultado_conversion.xlsx")

