# # =========================
# # batch_sender.py (VERSIÓN COMPLETA CON DEBUGGING)
# # =========================
# import io
# import asyncio
# import pandas as pd
# import aiohttp
# from typing import Dict, Any, List, Optional
# import json
# import math
# import openpyxl
# from openpyxl import load_workbook

# BATCH_SIZE = 500
# MAX_CONCURRENT_REQUESTS = 3

# class BatchSender:
#     def __init__(self):
#         self.batch_size = BATCH_SIZE
#         self.max_concurrent = MAX_CONCURRENT_REQUESTS
    
#     async def send_excel_in_batches(
#         self,
#         excel_bytes: bytes,
#         company_id: str,
#         price_list_id: str,
#         subsidiary_id: str,
#         id_warehouse: str,
#         id_country: str,
#         tax_code_country: str,
#         flag_use_simple_brand: bool,
#         base_url: str,
#         token: str
#     ) -> Dict[str, Any]:
#         """
#         Procesa un Excel en lotes y envía cada lote a la API de productos
#         SIN MODIFICAR EL EXCEL - usa openpyxl para extraer filas exactas
#         """
        
#         # ===== DEBUG: GUARDAR EXCEL ORIGINAL COMPLETO =====
#         with open("excel_original_completo_debug.xlsx", "wb") as f:
#             f.write(excel_bytes)
#         print("💾 Excel original guardado como 'excel_original_completo_debug.xlsx'")
#         # ==================================================
        
#         # Guardar el Excel original para trabajar con él
#         original_buffer = io.BytesIO(excel_bytes)
        
#         # Cargar el workbook con openpyxl (preserva formato exacto)
#         wb = load_workbook(original_buffer)
#         ws = wb.active
        
#         # Obtener headers (primera fila)
#         headers = []
#         for cell in ws[1]:
#             headers.append(cell.value)
        
#         total_rows = ws.max_row - 1  # Restar header
#         print("\n" + "="*80)
#         print("📊 EXCEL COMPLETO RECIBIDO")
#         print("="*80)
#         print(f"📊 Columnas del Excel original: {headers}")
#         print(f"📊 Total filas: {total_rows}")
#         print("="*80 + "\n")
        
#         # Calcular número de lotes
#         total_batches = math.ceil(total_rows / self.batch_size)
#         print(f"📦 Se procesarán {total_batches} lotes de {self.batch_size} productos cada uno")
        
#         results = {
#             "success": True,
#             "total_rows": total_rows,
#             "total_batches": total_batches,
#             "processed_rows": 0,
#             "successful_rows": 0,
#             "failed_rows": 0,
#             "batches": [],
#             "errors": []
#         }
        
#         # Crear semáforo para controlar concurrencia
#         semaphore = asyncio.Semaphore(self.max_concurrent)
        
#         async with aiohttp.ClientSession() as session:
#             tasks = []
            
#             for batch_num in range(total_batches):
#                 start_row = 2 + (batch_num * self.batch_size)  # +2 porque la fila 1 es header
#                 end_row = min(start_row + self.batch_size - 1, ws.max_row)
                
#                 # Crear tarea para este lote
#                 task = self._send_batch_openpyxl(
#                     session=session,
#                     semaphore=semaphore,
#                     batch_num=batch_num + 1,
#                     wb=wb,
#                     headers=headers,
#                     start_row=start_row,
#                     end_row=end_row,
#                     company_id=company_id,
#                     price_list_id=price_list_id,
#                     subsidiary_id=subsidiary_id,
#                     id_warehouse=id_warehouse,
#                     id_country=id_country,
#                     tax_code_country=tax_code_country,
#                     flag_use_simple_brand=flag_use_simple_brand,
#                     base_url=base_url,
#                     token=token,
#                     excel_bytes=excel_bytes  # 👈 PASAR EXCEL ORIGINAL PARA DEBUG
#                 )
#                 tasks.append(task)
            
#             # Esperar a que todos los lotes se procesen
#             batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
#             # Procesar resultados
#             for i, result in enumerate(batch_results):
#                 if isinstance(result, Exception):
#                     print(f"❌ Error fatal en lote {i+1}: {str(result)}")
#                     results["errors"].append({
#                         "batch": i + 1,
#                         "error": str(result)
#                     })
#                     results["failed_rows"] += self.batch_size
#                 else:
#                     results["batches"].append(result)
#                     results["processed_rows"] += result.get("processed_rows", 0)
#                     results["successful_rows"] += result.get("successful_rows", 0)
#                     results["failed_rows"] += result.get("failed_rows", 0)
        
#         print("\n" + "="*80)
#         print("📊 RESUMEN FINAL")
#         print("="*80)
#         print(f"✅ Total filas: {results['total_rows']}")
#         print(f"✅ Procesadas: {results['processed_rows']}")
#         print(f"✅ Exitosas: {results['successful_rows']}")
#         print(f"✅ Fallidas: {results['failed_rows']}")
#         print(f"✅ Errores: {len(results['errors'])}")
#         print("="*80 + "\n")
        
#         results["success"] = len(results["errors"]) == 0
#         return results
    
#     async def _send_batch_openpyxl(
#         self,
#         session: aiohttp.ClientSession,
#         semaphore: asyncio.Semaphore,
#         batch_num: int,
#         wb,
#         headers: List[str],
#         start_row: int,
#         end_row: int,
#         company_id: str,
#         price_list_id: str,
#         subsidiary_id: str,
#         id_warehouse: str,
#         id_country: str,
#         tax_code_country: str,
#         flag_use_simple_brand: bool,
#         base_url: str,
#         token: str,
#         excel_bytes: bytes = None  # 👈 NUEVO PARÁMETRO PARA DEBUG
#     ) -> Dict[str, Any]:
#         """Envía un lote usando openpyxl para preservar formato exacto"""
        
#         async with semaphore:
#             try:
#                 print(f"\n{'='*80}")
#                 print(f"📦 PROCESANDO LOTE {batch_num}")
#                 print(f"{'='*80}")
#                 print(f"📦 Lote {batch_num}: filas {start_row} a {end_row} ({end_row - start_row + 1} productos)")
                
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
#                         cell_value = wb.active.cell(row=wb_row, column=col_idx).value
#                         batch_ws.cell(row=rows_copied + 2, column=col_idx, value=cell_value)
#                     rows_copied += 1
                
#                 print(f"✅ {rows_copied} filas copiadas manteniendo formato original")
                
#                 # Guardar el workbook en un buffer
#                 excel_buffer = io.BytesIO()
#                 batch_wb.save(excel_buffer)
#                 excel_buffer.seek(0)
                
#                 # ===== DEBUG: GUARDAR EXCEL DEL LOTE =====
#                 with open(f"lote_{batch_num}_debug.xlsx", "wb") as f:
#                     f.write(excel_buffer.getvalue())
#                 print(f"💾 Lote {batch_num} guardado como 'lote_{batch_num}_debug.xlsx'")
                
#                 # Guardar Excel original si es el primer lote y tenemos los bytes
#                 if batch_num == 1 and excel_bytes:
#                     with open("excel_original_para_comparar.xlsx", "wb") as f:
#                         f.write(excel_bytes)
#                     print(f"💾 Excel original guardado como 'excel_original_para_comparar.xlsx'")
#                 # ==========================================
                
#                 # Construir URL
#                 url = f"{base_url}/api/excel/readexcel/{company_id}/pricelist/{price_list_id}/subsidiary/{subsidiary_id}"
#                 print(f"🌐 URL: {url}")
                
#                 # Crear FormData
#                 form_data = aiohttp.FormData()
#                 form_data.add_field(
#                     "file_excel",
#                     excel_buffer.read(),
#                     filename=f"batch_{batch_num}.xlsx",
#                     content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
#                 )
#                 form_data.add_field("idCountry", id_country)
#                 form_data.add_field("taxCodeCountry", tax_code_country)
#                 form_data.add_field("flagUseSimpleBrand", str(flag_use_simple_brand).lower())
#                 if id_warehouse:
#                     form_data.add_field("idWarehouse", id_warehouse)
                
#                 print(f"📤 Enviando lote {batch_num} a la API...")
                
#                 # Enviar solicitud
#                 async with session.post(
#                     url,
#                     data=form_data,
#                     headers={
#                         "Authorization": f"Bearer {token}"
#                     },
#                     timeout=aiohttp.ClientTimeout(total=60)
#                 ) as response:
                    
#                     response_text = await response.text()
                    
#                     print(f"📥 Código HTTP: {response.status}")
                    
#                     if response.status != 200:
#                         error_msg = f"Error {response.status}: {response_text[:500]}"
#                         print(f"❌ Lote {batch_num} falló: {error_msg}")
#                         return {
#                             "batch": batch_num,
#                             "success": False,
#                             "error": error_msg,
#                             "processed_rows": 0,
#                             "successful_rows": 0,
#                             "failed_rows": rows_copied
#                         }
                    
#                     # Intentar parsear JSON
#                     try:
#                         result = json.loads(response_text)
#                         print(f"📥 Respuesta API lote {batch_num}:")
#                         print(json.dumps(result, indent=2, ensure_ascii=False))
#                     except:
#                         result = {"message": response_text[:500]}
#                         print(f"📥 Respuesta no JSON: {response_text[:500]}")
                    
#                     # Extraer estadísticas
#                     successful = 0
#                     if isinstance(result, dict):
#                         if "data" in result and isinstance(result["data"], dict):
#                             successful = result["data"].get("n_products", 0)
                    
#                     print(f"✅ Lote {batch_num} completado: {successful} exitosos")
                    
#                     return {
#                         "batch": batch_num,
#                         "success": True,
#                         "response": result,
#                         "processed_rows": rows_copied,
#                         "successful_rows": successful,
#                         "failed_rows": 0,
#                         "errors": []
#                     }
                    
#             except Exception as e:
#                 print(f"❌ Error en lote {batch_num}: {str(e)}")
#                 import traceback
#                 traceback.print_exc()
#                 return {
#                     "batch": batch_num,
#                     "success": False,
#                     "error": str(e),
#                     "processed_rows": 0,
#                     "successful_rows": 0,
#                     "failed_rows": end_row - start_row + 1
#                 }

# # Instancia global
# batch_sender = BatchSender()