from fastapi import APIRouter
from .upload import router as excel_router
from .excel_conversion import router as conversion_router

router = APIRouter()
router.include_router(excel_router)
router.include_router(conversion_router)