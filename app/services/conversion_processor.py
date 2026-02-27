import pandas as pd
import re
import secrets
import string
import io
from typing import Set, Tuple, Dict, Optional

from .excel_cleaners import (
    normalize_text_value,
    clean_unit_value,
    IGV_FACTOR,
    process_product_code  # Añadir esta importación
)

# Constantes
ROW_ID_COL = "__ROW_ID__"

# ============================================================
# FUNCIONES DE LIMPIEZA (SOLO MODIFICAMOS limpiar_codigo_producto)
# ============================================================
def limpiar_valor_numerico(valor, default=0.0) -> float:
    if pd.isna(valor):
        return default
    
    s = str(valor).strip()
    if not s or s.upper() == "NAN" or s.upper() == "NULL":
        return default
    
    s_limpio = re.sub(r"[^\d.-]", "", s)
    
    try:
        num = float(s_limpio)
        if num < 0:
            return default
        return num
    except:
        return default


def generar_codigo_automatico(existentes: set) -> str:
    caracteres = string.ascii_uppercase + string.digits
    while True:
        codigo = "CM" + ''.join(secrets.choice(caracteres) for _ in range(10))
        if codigo not in existentes:
            existentes.add(codigo)
            return codigo


def limpiar_codigo_producto(valor, existentes: set) -> str:
    """
    Versión actualizada según nuevos requisitos:
    - Vacío → genérico
    - 2 dígitos → genérico
    - Duplicados → mantener original (no generar nuevo)
    """
    resultado = process_product_code(valor, existentes)
    return resultado["codigo_final"]


def limpiar_codigo_barra(valor, existentes: set) -> str:
    if pd.isna(valor) or not str(valor).strip():
        return ""
    
    s = str(valor).strip().upper()
    
    if re.search(r'[^A-Z0-9]', s):
        return ""
    
    if s.startswith('-'):
        return ""
    
    if len(s) < 4:
        return ""
    
    if s in existentes:
        return ""
    
    existentes.add(s)
    return s


def limpiar_marca_modelo(valor, default="S/M") -> str:
    if pd.isna(valor) or not str(valor).strip():
        return default
    
    s = str(valor).strip()
    if s.upper() == "NAN" or s.upper() == "NULL":
        return default
    
    return s


# ============================================================
# LECTURA DE EXCEL
# ============================================================
def leer_excel_conversion(input_path: str) -> pd.DataFrame:
    df_raw = pd.read_excel(input_path, header=None)
    
    headers = df_raw.iloc[3].fillna('').astype(str).str.strip().values
    data = df_raw.iloc[4:].copy()
    data.columns = headers
    data = data.dropna(how='all').reset_index(drop=True)
    data[ROW_ID_COL] = range(5, 5 + len(data))
    
    return data


def encontrar_columna_exacta(columnas, nombre_buscar):
    nombre_buscar_upper = nombre_buscar.upper()
    for i, col in enumerate(columnas):
        col_str = str(col) if pd.notna(col) else ""
        if col_str.upper() == nombre_buscar_upper:
            return i
    return None


# ============================================================
# FUNCIÓN PRINCIPAL (EXACTAMENTE IGUAL, solo usa la nueva limpiar_codigo_producto)
# ============================================================
def generar_excel_conversion_bytes(
    input_path: str, 
    selected_row_ids: set[int] = None,
    apply_igv_cost: bool = False,
    apply_igv_sale: bool = False,
    is_selva: bool = False,
    tienda_nombre: str = "Tienda1",
) -> tuple[bytes, dict]:
    
    # 1. Leer Excel
    df = leer_excel_conversion(input_path)
    before_rows = len(df)
    
    # 2. Filtrar duplicados si hay selección
    if selected_row_ids and "NOMBRE DEL PRODUCTO" in df.columns:
        s = df["NOMBRE DEL PRODUCTO"].astype(str).str.strip()
        dup_mask = s.ne("") & df["NOMBRE DEL PRODUCTO"].duplicated(keep=False)
        if dup_mask.any():
            dup_row_ids = set(df.loc[dup_mask, ROW_ID_COL].astype(int).tolist())
            keep_mask = (~df[ROW_ID_COL].isin(dup_row_ids)) | (df[ROW_ID_COL].isin(selected_row_ids))
            df = df.loc[keep_mask].copy().reset_index(drop=True)
    
    # 3. Crear diccionario de columnas por nombre exacto
    columnas_lista = list(df.columns)
    
    print("\n=== COLUMNAS ENCONTRADAS ===")
    for i, col in enumerate(columnas_lista):
        col_str = str(col) if pd.notna(col) else ""
        print(f"Columna {i}: '{col_str}'")
    
    # 4. Mapeo de columnas
    mapeo_columnas = {
        "código": "CODIGO DEL PRODUCTO",
        # "código barra": "CODIGO DE BARRA",
        "codigo padre": "CODIGO PADRE",
        "nombre": "NOMBRE DEL PRODUCTO",
        "descripcion": "DESCRIPCION",
        "categoria": "CATEGORIA",
        "precio costo": "PRECIO DE COSTO",
        "precio venta": "PRECIO DE VENTA PRINCIPAL",
        "unidad": "UNIDAD",
        "stock": "STOCK",
        "stock minimo": "STOCK MINIMO",
        "marca": "MARCA",
        "modelo": "MODELO",
        "almacenable": "ALMACENABLE",
        "RA precio venta": "PRECIO LISTA 2",
        "RA2 precio venta": "PRECIO LISTA 3"
    }
    
    # Encontrar índices
    indices_fijos = {}
    for col_destino, nombre_exacto in mapeo_columnas.items():
        idx = encontrar_columna_exacta(columnas_lista, nombre_exacto)
        if idx is not None:
            indices_fijos[col_destino] = idx
            print(f"✅ {nombre_exacto} → {col_destino} (columna {idx})")
        else:
            print(f"❌ {nombre_exacto} no encontrada")
    
    # 5. Identificar columnas de conversión
    idx_precio_lista_3 = encontrar_columna_exacta(columnas_lista, "PRECIO LISTA 3")
    inicio_conversion = (idx_precio_lista_3 + 1) if idx_precio_lista_3 is not None else (max(indices_fijos.values()) + 1 if indices_fijos else 0)
    
    columnas_conversion = {}
    for i in range(inicio_conversion, len(columnas_lista)):
        col_name = columnas_lista[i]
        if pd.notna(col_name) and str(col_name).strip() and col_name != ROW_ID_COL:
            nombre_limpio = normalize_text_value(col_name).replace(" ", "").replace("-", "")
            columnas_conversion[i] = nombre_limpio
            print(f"  ✅ Columna conversión {i}: {col_name} → {nombre_limpio}")
    
    # 6. Construir conversiones
    conversiones = []
    for idx in range(len(df)):
        partes = []
        for col_idx, nombre_conv in columnas_conversion.items():
            valor = df.iloc[idx, col_idx]
            if pd.notna(valor) and str(valor).strip() and str(valor).strip().upper() != "NAN":
                partes.append(f"{nombre_conv}-{nombre_conv}-{valor}")
        conversiones.append("#".join(partes))
    
    # 7. Función auxiliar
    def get_series(col_destino, default_value):
        if col_destino in indices_fijos:
            col_idx = indices_fijos[col_destino]
            return df.iloc[:, col_idx]
        else:
            return pd.Series([default_value] * len(df))
    
    # 8. Limpieza de códigos (AHORA USA LA NUEVA FUNCIÓN)
    codigos_existentes = set()
    codigo_series = get_series("código", "")
    codigos_limpios = []
    codes_fixed = 0
    codigos_info = []
    
    for idx, valor in enumerate(codigo_series):
        resultado = process_product_code(valor, codigos_existentes, idx)
        codigos_limpios.append(resultado["codigo_final"])
        if resultado["es_generico"]:
            codes_fixed += 1
        codigos_info.append({
            "fila": idx + 5,
            "original": resultado["codigo_original"],
            "final": resultado["codigo_final"],
            "es_generico": resultado["es_generico"],
            "razon": resultado["razon"]
        })
        print(f"  Fila {idx}: '{resultado['codigo_original']}' → '{resultado['codigo_final']}' ({resultado['razon']})")
    
    # Códigos de barra
    # codigos_barra_existentes = set()
    # codigo_barra_series = get_series("código barra", "")
    # codigos_barra_limpios = []
    
    # for idx, valor in enumerate(codigo_barra_series):
    #     limpio = limpiar_codigo_barra(valor, codigos_barra_existentes)
    #     codigos_barra_limpios.append(limpio)
    
    # 9. Crear DataFrame base con el ORDEN EXACTO de columnas
    df_base = pd.DataFrame()
    
    # 👇 ORDEN EXACTO DE COLUMNAS (22 columnas)
    # Para NOMBRE, solo convertir a mayúsculas sin limpieza de caracteres especiales
    df_base["nombre"] = get_series("nombre", "").apply(lambda x: str(x).upper() if pd.notna(x) else "")
    
    df_base["descripcion"] = get_series("descripcion", "")
    df_base["codigo padre"] = get_series("codigo padre", "")
    df_base["código"] = codigos_limpios
    # df_base["código barra"] = codigos_barra_limpios
    df_base["categoria"] = get_series("categoria", "SIN CATEGORIA")
    
    # Stock
    df_base["stock"] = get_series("stock", 0).apply(lambda x: limpiar_valor_numerico(x, 0.0))
    df_base["stock minimo"] = get_series("stock minimo", 0).apply(lambda x: limpiar_valor_numerico(x, 0.0))
    
    # Precios base
    precio_costo_base = get_series("precio costo", 0).apply(lambda x: limpiar_valor_numerico(x, 0.0))
    precio_venta_series = get_series("precio venta", 0).apply(lambda x: limpiar_valor_numerico(x, 1.0))
    
    # Validar precio venta vs costo
    precios_validados = []
    for i in range(len(df_base)):
        pv = precio_venta_series.iloc[i]
        pc = precio_costo_base.iloc[i]
        precios_validados.append(1.0 if pv < pc else pv)
    precio_venta_base = pd.Series(precios_validados)
    
    # Aplicar IGV
    if apply_igv_cost and not is_selva:
        df_base["precio costo"] = precio_costo_base * IGV_FACTOR
    else:
        df_base["precio costo"] = precio_costo_base
    
    if apply_igv_sale and not is_selva:
        df_base["precio venta"] = precio_venta_base * IGV_FACTOR
    else:
        df_base["precio venta"] = precio_venta_base
    
    # Porcentaje costo fijo
    df_base["porcentaje costo"] = 18
    
    # Columna conversion
    df_base["conversion"] = conversiones
    
    # Rangos fijos
    df_base["R-RANGO DE LISTA DE PRECIO 1"] = "0-0-0"
    
    # RA precio venta
    df_base["RA precio venta"] = get_series("RA precio venta", 0).apply(lambda x: limpiar_valor_numerico(x, 1.0))
    
    df_base["RA-RANGO LISTA DE PRECIO 2"] = "0-0-0"
    
    # RA2 precio venta
    df_base["RA2 precio venta"] = get_series("RA2 precio venta", 0).apply(lambda x: limpiar_valor_numerico(x, 1.0))
    
    df_base["RA2-RANGO LISTA DE PRECIO 2"] = "0-0-0"
    
    # Unidad, marca, modelo
    df_base["unidad"] = get_series("unidad", "").apply(lambda x: clean_unit_value(x) if pd.notna(x) else "UNIDAD")
    df_base["marca"] = get_series("marca", "").apply(lambda x: limpiar_marca_modelo(x, "S/M"))
    df_base["modelo"] = get_series("modelo", "").apply(lambda x: limpiar_marca_modelo(x, "S/M"))
    df_base["almacenable"] = get_series("almacenable", "si")
    
    # Usar el nombre de la tienda para la columna
    nombre_columna_tienda = f"W-{tienda_nombre}"
    df_base[nombre_columna_tienda] = df_base["stock"]
    
    # ===== AUDITORÍA =====
    errores = []
    ok_mask = []
    corregidos_mask = []
    
    def push_error(i, codigo, colname, valor, err, solucion, comentario):
        errores.append({
            "Código": codigo,
            "Ubicación (Fila / Columna)": f"{i+2} / {colname}",
            "Valor Detectado con error": valor,
            "Errores Detectados": err,
            "Solución Sugerida (Dato Listo)": solucion,
            "Comentarios": comentario,
        })
    
    for i in range(len(df_base)):
        ok = True
        corregido = False
        
        codigo = df_base.at[i, "código"]
        nombre = df_base.at[i, "nombre"]
        unidad = df_base.at[i, "unidad"]
        categoria = df_base.at[i, "categoria"]
        pc = float(df_base.at[i, "precio costo"])
        pv = float(df_base.at[i, "precio venta"])
        st = float(df_base.at[i, "stock"])
        
        if not str(codigo).strip():
            ok = False
            corregido = True
            push_error(i, codigo, "código", "", "CÓDIGO VACÍO", codigo, "Código generado automáticamente.")
        
        if not str(nombre).strip():
            ok = False
            push_error(i, codigo, "nombre", "", "NOMBRE VACÍO", "", "Nombre es obligatorio.")
        
        if str(categoria).strip() == "SIN CATEGORIA":
            push_error(i, codigo, "categoria", "", "CATEGORÍA VACÍA", "SIN CATEGORIA", "Se asignó default.")
        
        if st < 0:
            ok = False
            corregido = True
            push_error(i, codigo, "stock", st, "STOCK NEGATIVO", 0.0, "Se ajustó a 0.")
        
        if pc < 0:
            ok = False
            corregido = True
            push_error(i, codigo, "precio costo", pc, "PRECIO COSTO < 0", 0.0, "Se ajustó a 0.")
        
        if pv < 1:
            ok = False
            corregido = True
            push_error(i, codigo, "precio venta", pv, "PRECIO VENTA < 1", 1.0, "Se ajustó a 1.")
        
        if pv <= pc and pv >= 1:
            ok = False
            push_error(i, codigo, "precio venta", pv, "PRECIO VENTA <= PRECIO COSTO", pv, "Debe ser mayor que costo.")
        
        ok_mask.append(ok)
        corregidos_mask.append(corregido)
    
    # DataFrame de códigos procesados
    codigos_df = pd.DataFrame(codigos_info, columns=[
        "Fila", "Código Original", "Código Final", 
        "Es Genérico", "Razón"
    ])
    
    # Separar DataFrames
    productos_ok = df_base[pd.Series(ok_mask)].copy()
    productos_corregidos = df_base[pd.Series(corregidos_mask)].copy()
    
    # DataFrame de errores
    errores_df = pd.DataFrame(errores, columns=[
        "Código", "Ubicación (Fila / Columna)", "Valor Detectado con error",
        "Errores Detectados", "Solución Sugerida (Dato Listo)", "Comentarios"
    ])
    
    # 10. Crear Excel con 5 hojas
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        errores_df.to_excel(writer, index=False, sheet_name="Errores_Detectados")
        # codigos_df.to_excel(writer, index=False, sheet_name="Códigos_Procesados")
        productos_ok.to_excel(writer, index=False, sheet_name="Productos_OK")
        productos_corregidos.to_excel(writer, index=False, sheet_name="Productos_Corregidos")
        df_base.to_excel(writer, index=False, sheet_name="productos")
    
    stats = {
        "rows_before": before_rows,
        "rows_ok": len(productos_ok),
        "rows_corrected": len(productos_corregidos),
        "errors_count": len(errores_df),
        "codes_fixed": codes_fixed,
        "is_selva": is_selva
    }
    
    return out.getvalue(), stats













