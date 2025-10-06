import os
import uvicorn
import logging
import asyncio
from datetime import datetime, timezone, timedelta
import json
import threading
import time
from database import get_db, delete_old_request_logs

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Depends, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response
from sqlalchemy.orm import Session
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

# Kubernetes imports
from kubernetes import client, watch, stream
from kubernetes.client.rest import ApiException

# Local Imports
load_dotenv()
print("Current directory:", os.getcwd())
print("AWS_REGIONS =", os.getenv("AWS_REGIONS"))
print("AWS_TARGET_ACCOUNTS_ROLES =", os.getenv("AWS_TARGET_ACCOUNTS_ROLES"))

import database
from data_updater import update_all_data, update_single_cluster_data
from aws_data_fetcher import (
    upgrade_nodegroup_version,
    get_cluster_metrics,
    get_k8s_api_client,
    get_role_arn_for_account,
    fetch_access_entries_for_cluster,
    EKS_EOL_DATES,
)

# Scheduler & Concurrency Lock
scheduler = AsyncIOScheduler()
update_lock = asyncio.Lock()

async def trigger_update():
    if update_lock.locked():
        logging.warning("Data update is already in progress. Skipping scheduled run.")
        return
    async with update_lock:
        await update_all_data()

# Lifespan manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.info("Application starting up...")
    asyncio.create_task(trigger_update())
    scheduler.add_job(trigger_update, IntervalTrigger(hours=1))
    scheduler.start()
    logging.info("Scheduler started, running updates every 1 hour.")
    yield
    logging.info("Application shutting down...")
    scheduler.shutdown()

def start_cleanup_thread():
    def cleaner():
        while True:
            try:
                with next(get_db()) as session:
                    delete_old_request_logs(session)
                    print("[CLEANUP] Old request logs deleted successfully.")
            except Exception as e:
                print(f"[CLEANUP ERROR] {e}")
            time.sleep(86400)  # Sleep for 24 hours (86400 seconds)
    
    # Run the thread as a daemon so it doesn't block app shutdown
    thread = threading.Thread(target=cleaner, daemon=True)
    thread.start()

app = FastAPI(title="EKS Operational Dashboard", lifespan=lifespan)
start_cleanup_thread()  
database.create_db_and_tables()

# Middleware
@app.middleware("http")
async def log_and_db_session_middleware(request: Request, call_next):
    request.state.db = database.SessionLocal()
    try:
        request.state.last_update_time = database.get_last_update_time(request.state.db)
        sent_size = 0
        if request.method in ("POST", "PUT", "PATCH"):
            try:
                body = await request.body()
                sent_size = len(body)
                request._body = body
            except Exception:
                sent_size = 0
        response = await call_next(request)
        response_body = b""
        async for chunk in response.body_iterator:
            response_body += chunk
        database.log_request(
            request.state.db, request.method, str(request.url), request.client.host,
            sent_size, len(response_body), response.status_code
        )
        return Response(
            content=response_body, status_code=response.status_code,
            headers=dict(response.headers), media_type=response.media_type
        )
    finally:
        request.state.db.close()

class UserStateMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # REMOVED: Default test user
        request.state.user = request.session.get("user") 
        response = await call_next(request)
        return response

app.add_middleware(UserStateMiddleware)
app.add_middleware(SessionMiddleware, secret_key=os.getenv("SECRET_KEY", "a_very_secret_key_for_dev"))
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Routes
@app.get("/", response_class=HTMLResponse)
async def read_dashboard(request: Request, db: Session = Depends(database.get_db)):
    all_clusters = database.get_all_clusters_summary(db)
    now = datetime.now(timezone.utc)
    ninety_days_from_now = now + timedelta(days=90)
    
    quick_info = {
        "total_clusters": len(all_clusters),
        "clusters_with_health_issues": sum(1 for c in all_clusters if c["health_status_summary"] == "HAS_ISSUES"),
        "clusters_with_upgrade_insights_attention": sum(1 for c in all_clusters if c["upgrade_insight_status"] == "NEEDS_ATTENTION"),
        "clusters_nearing_eol_90_days": sum(1 for c in all_clusters if c["version"] and (eol := EKS_EOL_DATES.get(c["version"])) and now < eol <= ninety_days_from_now),
        "accounts_running_kubernetes_clusters": len({c["account_id"] for c in all_clusters}),
    }
    context = {"request": request, "clusters": all_clusters, "quick_info": quick_info, "errors": []}
    return templates.TemplateResponse("dashboard.html", context)

@app.get("/clusters", response_class=HTMLResponse)
async def list_clusters(request: Request, db: Session = Depends(database.get_db)):
    clusters_data = database.get_all_clusters_summary(db)
    context = {"request": request, "clusters": clusters_data}
    return templates.TemplateResponse("clusters.html", context)

@app.get("/clusters/{account_id}/{region}/{cluster_name}", response_class=HTMLResponse)
async def read_cluster_detail(request: Request, account_id: str, region: str, cluster_name: str, db: Session = Depends(database.get_db)):
    cluster_details = database.get_cluster_details(db, account_id, region, cluster_name)
    if not cluster_details:
        return templates.TemplateResponse("error.html", {"request": request, "errors": [f"Cluster {cluster_name} not found in database."]}, status_code=404)
    # Ensure Access Configuration (authentication mode) is populated for UI
    try:
        access_cfg = cluster_details.get('access_config') or {}
        if not access_cfg.get('authenticationMode'):
            from aws_data_fetcher import get_session
            role_arn = get_role_arn_for_account(account_id)
            session = get_session(role_arn)
            if session:
                eks_client = session.client('eks', region_name=region)
                desc = eks_client.describe_cluster(name=cluster_name).get('cluster', {})
                cluster_details['access_config'] = desc.get('accessConfig', {}) or { 'authenticationMode': None }
    except Exception as e:
        logging.warning(f"Could not fetch authentication mode for UI: {e}")

    context = {"request": request, "cluster": cluster_details, "account_id": account_id, "region": region}
    return templates.TemplateResponse("cluster_detail.html", context)

@app.post("/api/refresh-data")
async def refresh_data(request: Request):
    if update_lock.locked():
        return JSONResponse(status_code=409, content={"status": "error", "message": "Data update is already in progress."})
    asyncio.create_task(trigger_update())
    return JSONResponse(content={"status": "success", "message": "Full dashboard data refresh has been triggered."})

@app.post("/api/refresh-cluster/{account_id}/{region}/{cluster_name}")
async def refresh_cluster(account_id: str, region: str, cluster_name: str):
    # This endpoint now works correctly by calling the single-cluster update function
    db_session = database.SessionLocal()
    try:
        asyncio.create_task(update_single_cluster_data(db_session, account_id, region, cluster_name))
        return JSONResponse(content={"status": "success", "message": f"Refresh triggered for cluster {cluster_name}."})
    finally:
        # The task will run in the background, but we need to close the session we created here.
        # The task itself will manage its own session.
        pass # The task will close its own session

@app.post("/api/upgrade-nodegroup")
async def upgrade_nodegroup_api(request: Request):
    data = await request.json()
    account_id = data.get("accountId")
    role_arn = get_role_arn_for_account(account_id)
    result = upgrade_nodegroup_version(account_id, data.get("region"), data.get("clusterName"), data.get("nodegroupName"), role_arn)
    return JSONResponse(content=result, status_code=400 if "error" in result else 200)

@app.get("/api/metrics/{account_id}/{region}/{cluster_name}")
async def get_metrics_api(account_id: str, region: str, cluster_name: str):
    role_arn = get_role_arn_for_account(account_id)
    metrics = get_cluster_metrics(account_id, region, cluster_name, role_arn)
    return JSONResponse(content=metrics, status_code=500 if "error" in metrics else 200)

@app.get("/api/workloads/{account_id}/{region}/{cluster_name}", response_class=JSONResponse)
def get_workloads_api(account_id: str, region: str, cluster_name: str, db: Session = Depends(database.get_db)):
    """
    API endpoint to asynchronously fetch the large workloads JSON object.
    """
    workloads_data = db.query(database.Cluster.workloads).filter(
        database.Cluster.account_id == account_id,
        database.Cluster.region == region,
        database.Cluster.name == cluster_name
    ).scalar()

    if workloads_data:
        return JSONResponse(content=workloads_data)
    return JSONResponse(content={"error": "Workloads not found for this cluster."}, status_code=404)

# WebSockets
@app.websocket("/ws/logs/{account_id}/{region}/{cluster_name}/{namespace}/{pod_name}")
async def stream_logs(websocket: WebSocket, account_id: str, region: str, cluster_name: str, namespace: str, pod_name: str):
    await websocket.accept()
    db_session = database.SessionLocal()
    try:
        cluster = database.get_cluster_details(db_session, account_id, region, cluster_name)
        if not cluster: raise ValueError("Cluster not found in DB")
        
        # Check if cluster has private endpoint access
        networking = cluster.get("networking", {})
        endpoint_public_access = networking.get("endpointPublicAccess", True)
        if not endpoint_public_access:
            await websocket.send_text("ERROR: Cluster has private endpoint access only. Log streaming requires VPC access or public endpoint.")
            return
            
        api = get_k8s_api_client(cluster_name, cluster["endpoint"], cluster["certificateAuthority"]["data"], region, get_role_arn_for_account(account_id))
        core = client.CoreV1Api(api)
        streamer = stream.stream(core.read_namespaced_pod_log, name=pod_name, namespace=namespace, follow=True, _preload_content=False)
        while streamer.is_open():
            line = streamer.readline()
            if line: await websocket.send_text(line)
            else: await asyncio.sleep(0.1)
    except Exception as e:
        await websocket.send_text(f"ERROR: {e}")
        logging.error(f"Log stream error: {e}")
    finally:
        db_session.close()
        await websocket.close()

@app.websocket("/ws/events/{account_id}/{region}/{cluster_name}")
async def stream_events(websocket: WebSocket, account_id: str, region: str, cluster_name: str):
    await websocket.accept()
    db_session = database.SessionLocal()
    w = None
    try:
        cluster = database.get_cluster_details(db_session, account_id, region, cluster_name)
        if not cluster: raise ValueError("Cluster not found in DB")
        
        # Check if cluster has private endpoint access
        networking = cluster.get("networking", {})
        endpoint_public_access = networking.get("endpointPublicAccess", True)
        if not endpoint_public_access:
            await websocket.send_text(json.dumps({"type": "ERROR", "message": "Cluster has private endpoint access only. Event streaming requires VPC access or public endpoint."}))
            return
            
        api = get_k8s_api_client(cluster_name, cluster["endpoint"], cluster["certificateAuthority"]["data"], region, get_role_arn_for_account(account_id))
        core = client.CoreV1Api(api)
        w = watch.Watch()
        for event in w.stream(core.list_event_for_all_namespaces, timeout_seconds=3600):
            obj = api.sanitize_for_serialization(event["object"])
            await websocket.send_text(json.dumps({"type": event["type"], "object": obj}))
    except WebSocketDisconnect:
        logging.info(f"Disconnected: {cluster_name}")
    except Exception as e:
        await websocket.send_text(json.dumps({"type": "ERROR", "message": str(e)}))
        logging.error(f"Event stream error: {e}")
    finally:
        if w: w.stop()
        db_session.close()
        await websocket.close()



if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True, ws="wsproto")
