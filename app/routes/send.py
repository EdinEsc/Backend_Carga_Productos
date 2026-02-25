# # =========================
# # routes/send.py (AGREGAR NUEVO ENDPOINT)
# # =========================
# from fastapi import APIRouter, UploadFile, File, HTTPException, Request, BackgroundTasks
# from fastapi.responses import JSONResponse
# import aiohttp
# import asyncio
# import math
# import io
# import pandas as pd
# import openpyxl
# from openpyxl import load_workbook

# router = APIRouter(prefix="/send", tags=["Envío de Productos"])

# BATCH_SIZE = 500
# MAX_CONCURRENT = 3

# @router.post("/proxy")
# async def send_excel_proxy(
#     request: Request,
#     file_excel: UploadFile = File(...),
#     idCountry: str = File(...),
#     taxCodeCountry: str = File(...),
#     flagUseSimpleBrand: str = File(...),
#     idWarehouse: str = File(None)
# ):
#     """
#     Endpoint proxy que recibe el Excel, lo divide en lotes y los envía a la API original
#     """
    
#     # Obtener headers con la info original
#     original_url = request.headers.get("X-Original-Url")
#     token = request.headers.get("X-Token")
    
#     if not original_url or not token:
#         raise HTTPException(status_code=400, detail="Faltan headers requiredos")
    
#     # Leer el archivo
#     content = await file_excel.read()
    
#     # Procesar en lotes usando aiohttp
#     async with aiohttp.ClientSession() as session:
#         # Aquí implementas la lógica de lotes igual que antes
#         # pero usando el mismo formato de datos que el frontend original
        
#         # Leer el Excel para saber cuántas filas tiene
#         import pandas as pd
#         import io
        
#         df = pd.read_excel(io.BytesIO(content), engine="openpyxl")
#         total_rows = len(df)
#         total_batches = math.ceil(total_rows / BATCH_SIZE)
        
#         results = []
#         errors = []
        
#         # Crear semáforo para controlar concurrencia
#         semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        
#         async def send_batch(batch_num, start_idx, end_idx):
#             async with semaphore:
#                 try:
#                     # Extraer lote
#                     batch_df = df.iloc[start_idx:end_idx].copy()
                    
#                     # Crear Excel del lote
#                     excel_buffer = io.BytesIO()
#                     with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
#                         batch_df.to_excel(writer, index=False, sheet_name="productos")
#                     excel_buffer.seek(0)
                    
#                     # Crear FormData IGUAL que el frontend original
#                     data = aiohttp.FormData()
#                     data.add_field(
#                         "file_excel",
#                         excel_buffer.read(),
#                         filename=f"batch_{batch_num}.xlsx",
#                         content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
#                     )
#                     data.add_field("idCountry", idCountry)
#                     data.add_field("taxCodeCountry", taxCodeCountry)
#                     data.add_field("flagUseSimpleBrand", flagUseSimpleBrand)
#                     if idWarehouse:
#                         data.add_field("idWarehouse", idWarehouse)
                    
#                     # Enviar a la API original
#                     async with session.post(
#                         original_url,
#                         data=data,
#                         headers={"Authorization": f"Bearer {token}"}
#                     ) as response:
#                         response_text = await response.text()
                        
#                         if response.status != 200:
#                             return {
#                                 "batch": batch_num,
#                                 "success": False,
#                                 "error": response_text[:200]
#                             }
                        
#                         try:
#                             result = response_text
#                             return {
#                                 "batch": batch_num,
#                                 "success": True,
#                                 "response": result
#                             }
#                         except:
#                             return {
#                                 "batch": batch_num,
#                                 "success": True,
#                                 "response": response_text
#                             }
                            
#                 except Exception as e:
#                     return {
#                         "batch": batch_num,
#                         "success": False,
#                         "error": str(e)
#                     }
        
#         # Crear tareas para cada lote
#         tasks = []
#         for batch_num in range(total_batches):
#             start_idx = batch_num * BATCH_SIZE
#             end_idx = min(start_idx + BATCH_SIZE, total_rows)
#             tasks.append(send_batch(batch_num + 1, start_idx, end_idx))
        
#         # Ejecutar todas las tareas
#         batch_results = await asyncio.gather(*tasks)
        
#         # Procesar resultados
#         for result in batch_results:
#             if result["success"]:
#                 results.append(result)
#             else:
#                 errors.append(result)
        
#         # Combinar respuestas (opcional - puedes devolver todas)
#         if errors:
#             return JSONResponse(
#                 status_code=207,  # Multi-Status
#                 content={
#                     "success": False,
#                     "total_batches": total_batches,
#                     "successful_batches": len(results),
#                     "failed_batches": len(errors),
#                     "results": results,
#                     "errors": errors
#                 }
#             )
        
#         return JSONResponse(
#             content={
#                 "success": True,
#                 "total_batches": total_batches,
#                 "results": results
#             }
#         )
        
# @router.post("/direct")
# async def send_excel_direct(
#     request: Request,
#     file_excel: UploadFile = File(...),
#     idCountry: str = File(...),
#     taxCodeCountry: str = File(...),
#     flagUseSimpleBrand: str = File(...),
#     idWarehouse: str = File(None)
# ):
#     """
#     Endpoint temporal que envía el Excel completo SIN dividir en lotes
#     """
    
#     # Obtener headers
#     original_url = request.headers.get("X-Original-Url")
#     token = request.headers.get("X-Token")
    
#     if not original_url or not token:
#         raise HTTPException(status_code=400, detail="Faltan headers requiredos")
    
#     print(f"\n{'='*80}")
#     print(f"📥 Envío DIRECTO - Archivo: {file_excel.filename}")
#     print(f"📤 URL: {original_url}")
#     print(f"{'='*80}\n")
    
#     # Leer el archivo
#     content = await file_excel.read()
    
#     # Crear FormData exactamente igual que el frontend
#     data = aiohttp.FormData()
#     data.add_field(
#         "file_excel",
#         content,
#         filename=file_excel.filename,
#         content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
#     )
#     data.add_field("idCountry", idCountry)
#     data.add_field("taxCodeCountry", taxCodeCountry)
#     data.add_field("flagUseSimpleBrand", flagUseSimpleBrand)
#     if idWarehouse:
#         data.add_field("idWarehouse", idWarehouse)
    
#     # Enviar a la API original
#     async with aiohttp.ClientSession() as session:
#         async with session.post(
#             original_url,
#             data=data,
#             headers={"Authorization": f"Bearer {token}"}
#         ) as response:
            
#             response_text = await response.text()
            
#             print(f"📥 Código HTTP: {response.status}")
#             print(f"📥 Respuesta:")
#             print(response_text)
            
#             # Intentar parsear JSON
#             try:
#                 result = response_text
#                 return JSONResponse(
#                     status_code=response.status,
#                     content={
#                         "success": response.status == 200,
#                         "status_code": response.status,
#                         "response": result
#                     }
#                 )
#             except:
#                 return JSONResponse(
#                     status_code=response.status,
#                     content={
#                         "success": response.status == 200,
#                         "status_code": response.status,
#                         "response": response_text
#                     }
#                 )
                
# @router.post("/batch")
# async def send_excel_batch(
#     request: Request,
#     file_excel: UploadFile = File(...),
#     idCountry: str = File(...),
#     taxCodeCountry: str = File(...),
#     flagUseSimpleBrand: str = File(...),
#     idWarehouse: str = File(None)
# ):
#     """
#     Endpoint que envía en lotes PRESERVANDO el formato exacto del Excel
#     """
    
#     # Obtener headers
#     original_url = request.headers.get("X-Original-Url")
#     token = request.headers.get("X-Token")
    
#     if not original_url or not token:
#         raise HTTPException(status_code=400, detail="Faltan headers requeridos")
    
#     print(f"\n{'='*80}")
#     print(f"📥 Envío por LOTES - Archivo: {file_excel.filename}")
#     print(f"📤 URL: {original_url}")
#     print(f"{'='*80}\n")
    
#     # Leer el archivo
#     content = await file_excel.read()
    
#     # Guardar el Excel original para trabajar con él
#     original_buffer = io.BytesIO(content)
    
#     # Cargar el workbook con openpyxl (preserva formato exacto)
#     wb = load_workbook(original_buffer)
#     ws = wb.active
    
#     # Obtener headers (primera fila)
#     headers = []
#     for cell in ws[1]:
#         headers.append(cell.value)
    
#     total_rows = ws.max_row - 1  # Restar header
#     total_batches = math.ceil(total_rows / BATCH_SIZE)
    
#     print(f"📊 Total filas: {total_rows}")
#     print(f"📦 Total lotes: {total_batches}")
#     print(f"📊 Headers: {headers}")
    
#     results = []
#     errors = []
    
#     # Crear semáforo para controlar concurrencia
#     semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    
#     async def send_batch(batch_num, start_row, end_row):
#         async with semaphore:
#             try:
#                 print(f"\n{'='*60}")
#                 print(f"📦 Procesando lote {batch_num}")
#                 print(f"{'='*60}")
#                 print(f"Filas {start_row} a {end_row} ({end_row - start_row + 1} productos)")
                
#                 # Crear un nuevo workbook para este lote
#                 batch_wb = openpyxl.Workbook()
#                 batch_ws = batch_wb.active
#                 batch_ws.title = "productos"
                
#                 # Copiar headers
#                 for col_idx, header in enumerate(headers, 1):
#                     batch_ws.cell(row=1, column=col_idx, value=header)
                
#                 # Copiar filas del lote
#                 rows_copied = 0
#                 for wb_row in range(start_row, end_row + 1):
#                     for col_idx in range(1, len(headers) + 1):
#                         cell_value = ws.cell(row=wb_row, column=col_idx).value
#                         batch_ws.cell(row=rows_copied + 2, column=col_idx, value=cell_value)
#                     rows_copied += 1
                
#                 print(f"✅ {rows_copied} filas copiadas")
                
#                 # Guardar el workbook en un buffer
#                 excel_buffer = io.BytesIO()
#                 batch_wb.save(excel_buffer)
#                 excel_buffer.seek(0)
                
#                 # Crear FormData IGUAL que el envío directo
#                 data = aiohttp.FormData()
#                 data.add_field(
#                     "file_excel",
#                     excel_buffer.read(),
#                     filename=f"batch_{batch_num}.xlsx",
#                     content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
#                 )
#                 data.add_field("idCountry", idCountry)
#                 data.add_field("taxCodeCountry", taxCodeCountry)
#                 data.add_field("flagUseSimpleBrand", flagUseSimpleBrand)
#                 if idWarehouse:
#                     data.add_field("idWarehouse", idWarehouse)
                
#                 print(f"📤 Enviando lote {batch_num}...")
                
#                 # Enviar a la API original
#                 async with session.post(
#                     original_url,
#                     data=data,
#                     headers={"Authorization": f"Bearer {token}"}
#                 ) as response:
#                     response_text = await response.text()
                    
#                     print(f"📥 Código HTTP: {response.status}")
                    
#                     if response.status != 200:
#                         print(f"❌ Error en lote {batch_num}")
#                         return {
#                             "batch": batch_num,
#                             "success": False,
#                             "error": response_text[:500],
#                             "status_code": response.status
#                         }
                    
#                     # Intentar parsear JSON
#                     try:
#                         result = response_text
#                         print(f"✅ Lote {batch_num} completado")
#                         print(f"   Respuesta: {response_text}")
                        
#                         return {
#                             "batch": batch_num,
#                             "success": True,
#                             "response": result
#                         }
#                     except:
#                         return {
#                             "batch": batch_num,
#                             "success": True,
#                             "response": response_text
#                         }
                            
#             except Exception as e:
#                 print(f"❌ Excepción en lote {batch_num}: {str(e)}")
#                 import traceback
#                 traceback.print_exc()
#                 return {
#                     "batch": batch_num,
#                     "success": False,
#                     "error": str(e)
#                 }
    
#     async with aiohttp.ClientSession() as session:
#         # Crear tareas para cada lote
#         tasks = []
#         for batch_num in range(total_batches):
#             start_row = 2 + (batch_num * BATCH_SIZE)
#             end_row = min(start_row + BATCH_SIZE - 1, ws.max_row)
#             tasks.append(send_batch(batch_num + 1, start_row, end_row))
        
#         # Ejecutar todas las tareas
#         batch_results = await asyncio.gather(*tasks)
        
#         # Procesar resultados
#         total_products = 0
#         for result in batch_results:
#             if result["success"]:
#                 results.append(result)
#                 # Intentar extraer n_products de la respuesta
#                 try:
#                     if isinstance(result["response"], str):
#                         import json
#                         resp_json = json.loads(result["response"])
#                         if "data" in resp_json and "n_products" in resp_json["data"]:
#                             total_products += resp_json["data"]["n_products"]
#                 except:
#                     pass
#             else:
#                 errors.append(result)
        
#         print(f"\n{'='*80}")
#         print(f"📊 RESUMEN FINAL")
#         print(f"{'='*80}")
#         print(f"✅ Total lotes: {total_batches}")
#         print(f"✅ Exitosos: {len(results)}")
#         print(f"❌ Fallidos: {len(errors)}")
#         print(f"📦 Total productos: {total_products}")
        
#         return JSONResponse(
#             content={
#                 "success": len(errors) == 0,
#                 "total_batches": total_batches,
#                 "successful_batches": len(results),
#                 "failed_batches": len(errors),
#                 "total_products": total_products,
#                 "results": results,
#                 "errors": errors
#             }
#         )