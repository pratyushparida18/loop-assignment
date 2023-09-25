from fastapi import APIRouter, WebSocket,BackgroundTasks
from app.controllers.app_controllers import trigger_report_controller,get_report_controller
router = APIRouter()

@router.websocket('/trigger_report')
async def trigger_report_router(websocket: WebSocket,background_tasks: BackgroundTasks):
    return await trigger_report_controller(websocket,background_tasks)

@router.get('/get_report')
async def get_report_router(report_id: str):
    return await get_report_controller(report_id)
