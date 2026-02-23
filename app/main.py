

# from fastapi import FastAPI
# from fastapi.middleware.cors import CORSMiddleware
# from app.routes.upload import router as upload_router
# from app.routes.excel_conversion import router as conversion_router # 👈 AÑADIR

# app = FastAPI()

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=[
#         "http://localhost:5173",
#         "http://127.0.0.1:5173",
#     ],
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
# app.include_router(conversion_router)  # 👈 AÑADIR



# app/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routes.upload import router as upload_router
from app.routes.excel_conversion import router as conversion_router
from app.routes.dev_session_proxy import router as dev_router  # /dev/session, /dev/user, /dev/warehouses, etc.

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
app.include_router(conversion_router)
app.include_router(dev_router)