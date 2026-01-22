# # =========================
# # app/main.py
# # (REEMPLAZA TODO el archivo por este contenido)
# # =========================

# from fastapi import FastAPI
# from fastapi.middleware.cors import CORSMiddleware
# from app.routes.upload import router as upload_router

# app = FastAPI()

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["http://localhost:5173"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
#     expose_headers=[
#         "X-Rows-Before",
#         "X-Rows-OK",
#         "X-Rows-Corrected",
#         "X-Errors-Count",
#         "X-Codes-Fixed",
#         "Content-Disposition",
#     ],
# )

# app.include_router(upload_router)


from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes.upload import router as upload_router

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=[
        "X-Rows-Before",
        "X-Rows-OK",
        "X-Rows-Corrected",
        "X-Errors-Count",
        "X-Codes-Fixed",
        "Content-Disposition",
    ],
)

app.include_router(upload_router)
