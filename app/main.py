# from fastapi import FastAPI
# from fastapi.middleware.cors import CORSMiddleware

# from app.routes.upload import router as upload_router
# from app.routes.excel_conversion import router as conversion_router



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
# app.include_router(conversion_router)





from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routes import router

app = FastAPI(title="Excel Processor API")

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

app.include_router(router)

@app.get("/")
async def root():
    return {"message": "Excel Processor API", "version": "1.0"}