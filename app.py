import os,threading,datetime,uuid
from flask import Flask,render_template_string,redirect,url_for,request,jsonify
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
from email_client import fetch_and_process_invoices
from storage_local import upload_invoice as save_invoice_file
from invoice_db import get_dashboard_stats,get_graph_data,get_recent_invoices,get_invoice_by_id,get_exception_invoices,get_payable_invoices
from payments import create_payment_intent_for_invoices, mark_payment_failed_or_canceled, confirm_payment_intent, list_pending_payment_confirmations, list_payment_history
import stripe
from vendor_db import get_vendors,get_all_vendors_detailed,get_vendor_stats,get_vendor_by_id_detailed,create_vendor,delete_vendor
from chat_db import create_chat as db_create_chat, list_messages as db_list_messages, add_message as db_add_message, get_chat_vendor, list_chats_for_vendor as db_list_chats
from chat_llm import generate_vendor_response, generate_chat_title
from db import get_conn
from flask import Response, stream_with_context
from po_db import get_po_by_id as get_po_detail
from document_pipeline import process_saved_invoice_file
from ingress_tracking import register_ingress_source_document_best_effort
from agent_worker import start_agent_worker_thread
from realtime_events import publish_live_update, stream_live_updates
from agent_db import (
    get_agent_operations_metrics,
    get_agent_operations_overview,
    get_invoice_audit_trail,
    get_source_document_detail,
    get_vendor_communication,
    list_agent_decisions,
    list_agent_tasks,
    list_human_review_items,
    list_sla_breaches,
    list_sla_configs,
    list_source_documents,
    list_vendor_communications,
    list_workflow_history,
    list_workflow_states,
)
from sla_monitor_agent import run_sla_monitor_once
from vendor_communication_agent import (
    approve_vendor_communication,
    create_vendor_communication_draft,
    mark_vendor_communication_sent,
    reject_vendor_communication,
)
from payment_authorization import (
    approve_payment_authorization,
    evaluate_and_route_payment_batch,
    execute_payment_authorization,
    get_payment_authorization,
    list_payment_authorizations,
    reject_payment_authorization,
    request_payment_authorization,
    submit_payment_authorization_request,
    submit_payment_execution,
    submit_payment_route,
)
from review_queue_service import (
    assign_review_item,
    get_review_queue_counts,
    reject_review_item,
    resolve_review_item,
)
from user_facing_errors import get_error_details, get_user_facing_message

load_dotenv()

CHECK_INTERVAL_SECONDS=int(os.getenv("CHECK_INTERVAL_SECONDS","30"))

app=Flask(__name__)
app.secret_key=os.getenv("FLASK_SECRET_KEY","change-me")

last_run_at=None
last_run_result=""
is_running=False
lock=threading.Lock()


def _parse_bool_arg(name, default=False):
    value=request.args.get(name)
    if value is None:
        return default
    return str(value).strip().lower() in {"1","true","yes","on"}


def _parse_limit_arg(default=100, max_value=500):
    try:
        value=int(request.args.get("limit", default))
    except Exception:
        value=default
    return max(1,min(value,max_value))


def _agent_api_response(loader, *, not_found_message=None):
    try:
        payload=loader()
        if payload is None and not_found_message:
            return jsonify({"error": not_found_message}), 404
        return jsonify(payload)
    except Exception as e:
        return _error_response(e, default_status=503 if isinstance(e, RuntimeError) else 500)


def _agent_mutation_response(loader, *, created=False):
    try:
        payload=loader()
        return jsonify(payload), (201 if created else 200)
    except Exception as e:
        default_status = 503 if isinstance(e, RuntimeError) else 400 if isinstance(e, ValueError) else 500
        return _error_response(e, default_status=default_status)


def _error_response(exc, *, default_status=500):
    details = get_error_details(exc, default_status=default_status)
    return jsonify({"error": details.message, "errorCode": details.code}), details.status_code

INDEX_TEMPLATE="""
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Invoice Processor</title>
<style>
body{font-family:Arial,sans-serif;margin:40px}
nav a{margin-right:15px;text-decoration:none;color:#0366d6}
nav a:hover{text-decoration:underline}
.status{margin-bottom:20px}
pre{background:#f4f4f4;padding:10px;border-radius:4px;white-space:pre-wrap}
button{padding:10px 16px;font-size:14px;cursor:pointer}
button[disabled]{opacity:.5;cursor:not-allowed}
</style>
</head>
<body>
<nav>
<a href="{{ url_for('index') }}">Email intake</a>
<a href="{{ url_for('upload_page') }}">Upload invoice</a>
</nav>
<h1>Email Invoice Downloader</h1>
<div class="status">
<p><strong>Auto check interval:</strong> every {{ interval }} seconds.</p>
<p><strong>Job status:</strong> {% if is_running %}Running...{% else %}Idle{% endif %}</p>
<p><strong>Last run at:</strong> {% if last_run_at %}{{ last_run_at }}{% else %}Never{% endif %}</p>
</div>
<form method="post" action="{{ url_for('run_now') }}">
<button type="submit" {% if is_running %}disabled{% endif %}>Run Invoice Check Now</button>
</form>
<h2>Last Run Log</h2>
<pre>{{ last_run_result }}</pre>
</body>
</html>
"""

UPLOAD_TEMPLATE="""
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Upload Invoice</title>
<style>
body{font-family:Arial,sans-serif;margin:40px}
nav a{margin-right:15px;text-decoration:none;color:#0366d6}
nav a:hover{text-decoration:underline}
form{margin-bottom:20px}
label{display:block;margin-top:10px}
select,input[type=file]{margin-top:4px}
pre{background:#f4f4f4;padding:10px;border-radius:4px;white-space:pre-wrap}
button{padding:10px 16px;font-size:14px;cursor:pointer}
button[disabled]{opacity:.5;cursor:not-allowed}
.error{color:#b00020}
</style>
</head>
<body>
<nav>
<a href="{{ url_for('index') }}">Email intake</a>
<a href="{{ url_for('upload_page') }}">Upload invoice</a>
</nav>
<h1>Upload Invoice</h1>
{% if error %}
<p class="error">{{ error }}</p>
{% endif %}
<form method="post" enctype="multipart/form-data">
<label>Vendor
<select name="vendor_id" required>
<option value="">Select vendor</option>
{% for v in vendors %}
<option value="{{ v[0] }}" {% if selected_vendor_id==v[0] %}selected{% endif %}>{{ v[1] }}</option>
{% endfor %}
</select>
</label>
<label>Invoice file
<input type="file" name="file" required>
</label>
<button type="submit" id="uploadBtn">Upload and process</button>
</form>
<h2>Upload Log</h2>
<pre>{{ logs }}</pre>
<script>
const f=document.querySelector("form");
const b=document.getElementById("uploadBtn");
if(f && b){
  f.addEventListener("submit",function(){
    b.disabled=true;
  });
}
</script>

</body>
</html>
"""

def run_job(triggered_by="scheduler"):
    global last_run_at,last_run_result,is_running
    with lock:
        if is_running:
            return
        is_running=True
    publish_live_update("intake.run_started", {"triggeredBy": triggered_by})
    try:
        last_run_at=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logs=fetch_and_process_invoices()
        if isinstance(logs,list):
            last_run_result="\n".join(logs)
        else:
            last_run_result=str(logs)
    except Exception as e:
        last_run_result=get_user_facing_message(e)
    finally:
        is_running=False
        publish_live_update(
            "intake.run_finished",
            {
                "triggeredBy": triggered_by,
                "lastRunAt": last_run_at,
                "isRunning": is_running,
            },
        )

@app.route("/",methods=["GET"])
def index():
    return render_template_string(
        INDEX_TEMPLATE,
        last_run_at=last_run_at,
        last_run_result=last_run_result,
        is_running=is_running,
        interval=CHECK_INTERVAL_SECONDS,
    )

@app.route("/run-now",methods=["POST"])
def run_now():
    if not is_running:
        t=threading.Thread(target=run_job,args=("manual",),daemon=True)
        t.start()
    return redirect(url_for("index"))

@app.route("/api/run/status", methods=["GET"])
def api_run_status():
    """Return current background run status and last logs for the dashboard."""
    try:
        return jsonify({
            "isRunning": bool(is_running),
            "lastRunAt": last_run_at,
            "lastRunResult": last_run_result,
            "intervalSeconds": CHECK_INTERVAL_SECONDS,
        })
    except Exception as e:
        return _error_response(e)


@app.route("/api/live/stream", methods=["GET"])
def api_live_stream():
    headers = {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    return Response(stream_with_context(stream_live_updates(heartbeat_seconds=1.0)), headers=headers)

@app.route("/upload",methods=["GET","POST"])
def upload_page():
    vendors=get_vendors()
    logs=[]
    error=""
    selected_vendor_id=""
    if request.method=="POST":
        selected_vendor_id=request.form.get("vendor_id") or ""
        file=request.files.get("file")
        if not selected_vendor_id:
            error="Vendor is required"
        elif not file or file.filename=="":
            error="File is required"
        else:
            try:
                message_id="upload-"+uuid.uuid4().hex
                payload=file.read()
                full_path,uploaded=save_invoice_file("upload_vendor_"+selected_vendor_id,message_id,file.filename,payload)
                if uploaded:
                    logs.append(f"Saved upload to {full_path}")
                    source_document, registration_warning = register_ingress_source_document_best_effort(
                        source_type="manual_upload",
                        storage_path=full_path,
                        source_ref=message_id,
                        original_filename=file.filename,
                        content_type=getattr(file, "mimetype", None),
                        content_bytes=payload,
                        vendor_id=selected_vendor_id,
                        metadata={
                            "upload": {
                                "message_id": message_id,
                            }
                        },
                    )
                    if source_document:
                        logs.append(f"Registered source document {source_document['id']} for upload")
                    elif registration_warning:
                        logs.append(f"Source document registration skipped for upload: {registration_warning}")
                    result=process_saved_invoice_file(
                        full_path,
                        from_email=None,
                        message_id=message_id,
                        vendor_id_override=selected_vendor_id,
                        original_filename=file.filename,
                        source_document_id=source_document["id"] if source_document else None,
                    )
                    logs.extend(result.get("logs") or [])
                    publish_live_update(
                        "invoice.upload_processed",
                        {
                            "sourceDocumentId": source_document["id"] if source_document else None,
                            "invoiceIds": result.get("invoice_ids") or [],
                        },
                    )
                else:
                    logs.append("This exact file was already uploaded earlier, so it was not processed again.")
            except Exception as e:
                logs.append(get_user_facing_message(e))
    logs_text="\n".join(logs) if logs else ""
    return render_template_string(
        UPLOAD_TEMPLATE,
        vendors=vendors,
        logs=logs_text,
        error=error,
        selected_vendor_id=selected_vendor_id,
    )

# API Routes for Dashboard Frontend
@app.route("/api/dashboard/stats",methods=["GET"])
def api_dashboard_stats():
    """Get dashboard statistics"""
    try:
        stats = get_dashboard_stats(30)
        return jsonify(stats)
    except Exception as e:
        return _error_response(e)

@app.route("/api/dashboard/graph-data",methods=["GET"])
def api_graph_data():
    """Get graph data for last 30 days"""
    try:
        data = get_graph_data(30)
        return jsonify(data)
    except Exception as e:
        return _error_response(e)

@app.route("/api/invoices/recent",methods=["GET"])
def api_recent_invoices():
    """Get recent invoices"""
    try:
        limit = request.args.get("limit", default=10, type=int)
        invoices = get_recent_invoices(limit)
        return jsonify(invoices)
    except Exception as e:
        return _error_response(e)

@app.route("/api/invoices/<invoice_id>",methods=["GET"])
def api_invoice_detail(invoice_id):
    """Get specific invoice details"""
    try:
        invoice = get_invoice_by_id(invoice_id)
        if invoice:
            return jsonify(invoice)
        else:
            return jsonify({"error": "Invoice not found"}), 404
    except Exception as e:
        return _error_response(e)


@app.route("/api/invoices/<invoice_id>/audit-trail", methods=["GET"])
def api_invoice_audit_trail(invoice_id):
    return _agent_api_response(
        lambda: get_invoice_audit_trail(invoice_id, limit=_parse_limit_arg(500, max_value=1000)),
        not_found_message="Invoice audit trail not found",
    )


@app.route("/api/agent/overview", methods=["GET"])
def api_agent_overview():
    return _agent_api_response(get_agent_operations_overview)


@app.route("/api/agent/operations/metrics", methods=["GET"])
def api_agent_operations_metrics():
    return _agent_api_response(
        lambda: get_agent_operations_metrics(
            window_days=request.args.get("days", default=30, type=int),
        )
    )


@app.route("/api/agent/source-documents", methods=["GET"])
def api_agent_source_documents():
    return _agent_api_response(
        lambda: list_source_documents(
            ingestion_status=request.args.get("ingestion_status") or request.args.get("ingestionStatus"),
            segmentation_status=request.args.get("segmentation_status") or request.args.get("segmentationStatus"),
            extraction_status=request.args.get("extraction_status") or request.args.get("extractionStatus"),
            vendor_id=request.args.get("vendor_id") or request.args.get("vendorId"),
            source_type=request.args.get("source_type") or request.args.get("sourceType"),
            limit=_parse_limit_arg(100),
        )
    )


@app.route("/api/agent/source-documents/<source_document_id>", methods=["GET"])
def api_agent_source_document_detail(source_document_id):
    return _agent_api_response(
        lambda: get_source_document_detail(source_document_id),
        not_found_message="Source document not found",
    )


@app.route("/api/agent/workflow-states", methods=["GET"])
def api_agent_workflow_states():
    return _agent_api_response(
        lambda: list_workflow_states(
            entity_type=request.args.get("entity_type") or request.args.get("entityType"),
            current_state=request.args.get("current_state") or request.args.get("currentState"),
            current_stage=request.args.get("current_stage") or request.args.get("currentStage"),
            limit=_parse_limit_arg(100),
        )
    )


@app.route("/api/agent/workflow-history/<entity_type>/<entity_id>", methods=["GET"])
def api_agent_workflow_history(entity_type, entity_id):
    return _agent_api_response(
        lambda: list_workflow_history(
            entity_type,
            entity_id,
            limit=_parse_limit_arg(100),
        )
    )


@app.route("/api/agent/tasks", methods=["GET"])
def api_agent_tasks():
    return _agent_api_response(
        lambda: list_agent_tasks(
            status=request.args.get("status"),
            task_type=request.args.get("task_type") or request.args.get("taskType"),
            entity_type=request.args.get("entity_type") or request.args.get("entityType"),
            entity_id=request.args.get("entity_id") or request.args.get("entityId"),
            source_document_id=request.args.get("source_document_id") or request.args.get("sourceDocumentId"),
            retries_only=_parse_bool_arg("retries_only") or _parse_bool_arg("retriesOnly"),
            limit=_parse_limit_arg(100),
        )
    )


@app.route("/api/agent/decisions", methods=["GET"])
def api_agent_decisions():
    return _agent_api_response(
        lambda: list_agent_decisions(
            entity_type=request.args.get("entity_type") or request.args.get("entityType"),
            entity_id=request.args.get("entity_id") or request.args.get("entityId"),
            task_id=request.args.get("task_id") or request.args.get("taskId"),
            source_document_id=request.args.get("source_document_id") or request.args.get("sourceDocumentId"),
            limit=_parse_limit_arg(100),
        )
    )


@app.route("/api/agent/review-queue", methods=["GET"])
def api_agent_review_queue():
    return _agent_api_response(
        lambda: list_human_review_items(
            status=request.args.get("status"),
            active_only=_parse_bool_arg("active_only") or _parse_bool_arg("activeOnly"),
            queue_name=request.args.get("queue_name") or request.args.get("queueName"),
            entity_type=request.args.get("entity_type") or request.args.get("entityType"),
            entity_id=request.args.get("entity_id") or request.args.get("entityId"),
            source_document_id=request.args.get("source_document_id") or request.args.get("sourceDocumentId"),
            invoice_id=request.args.get("invoice_id") or request.args.get("invoiceId"),
            assigned_to=request.args.get("assigned_to") or request.args.get("assignedTo"),
            limit=_parse_limit_arg(100),
        )
    )


@app.route("/api/agent/review-queue/counts", methods=["GET"])
def api_agent_review_queue_counts():
    return _agent_api_response(get_review_queue_counts)


@app.route("/api/agent/review-queue/<review_item_id>/assign", methods=["POST"])
def api_agent_review_queue_assign(review_item_id):
    def loader():
        data = request.get_json(force=True) or {}
        reviewer = (data.get("reviewer") or data.get("assignedTo") or data.get("assigned_to") or "").strip()
        if not reviewer:
            raise ValueError("reviewer is required")
        return assign_review_item(review_item_id, reviewer=reviewer)

    return _agent_mutation_response(loader)


@app.route("/api/agent/review-queue/<review_item_id>/resolve", methods=["POST"])
def api_agent_review_queue_resolve(review_item_id):
    def loader():
        data = request.get_json(force=True) or {}
        reviewer = (data.get("reviewer") or data.get("resolvedBy") or data.get("resolved_by") or "").strip()
        action = (data.get("action") or "").strip()
        resolution_notes = (data.get("resolutionNotes") or data.get("resolution_notes") or "").strip()
        selected_po_id = data.get("selectedPoId") or data.get("selected_po_id")
        if not reviewer:
            raise ValueError("reviewer is required")
        if not action:
            raise ValueError("action is required")
        if not resolution_notes:
            raise ValueError("resolutionNotes is required")
        return resolve_review_item(
            review_item_id,
            reviewer=reviewer,
            action=action,
            resolution_notes=resolution_notes,
            selected_po_id=selected_po_id,
        )

    return _agent_mutation_response(loader)


@app.route("/api/agent/review-queue/<review_item_id>/reject", methods=["POST"])
def api_agent_review_queue_reject(review_item_id):
    def loader():
        data = request.get_json(force=True) or {}
        reviewer = (data.get("reviewer") or data.get("rejectedBy") or data.get("rejected_by") or "").strip()
        resolution_notes = (data.get("resolutionNotes") or data.get("resolution_notes") or "").strip()
        if not reviewer:
            raise ValueError("reviewer is required")
        if not resolution_notes:
            raise ValueError("resolutionNotes is required")
        return reject_review_item(
            review_item_id,
            reviewer=reviewer,
            resolution_notes=resolution_notes,
        )

    return _agent_mutation_response(loader)


@app.route("/api/agent/sla-configs", methods=["GET"])
def api_agent_sla_configs():
    return _agent_api_response(
        lambda: list_sla_configs(
            entity_type=request.args.get("entity_type") or request.args.get("entityType"),
            active_only=_parse_bool_arg("active_only") or _parse_bool_arg("activeOnly"),
        )
    )


@app.route("/api/agent/sla-breaches", methods=["GET"])
def api_agent_sla_breaches():
    return _agent_api_response(
        lambda: list_sla_breaches(
            entity_type=request.args.get("entity_type") or request.args.get("entityType"),
            current_state=request.args.get("current_state") or request.args.get("currentState"),
            limit=_parse_limit_arg(100),
        )
    )


@app.route("/api/agent/sla-monitor/run", methods=["POST"])
def api_agent_sla_monitor_run():
    return _agent_mutation_response(lambda: run_sla_monitor_once(triggered_by="manual"))


@app.route("/api/agent/vendor-communications", methods=["GET"])
def api_agent_vendor_communications():
    return _agent_api_response(
        lambda: list_vendor_communications(
            status=request.args.get("status"),
            direction=request.args.get("direction"),
            vendor_id=request.args.get("vendor_id") or request.args.get("vendorId"),
            invoice_id=request.args.get("invoice_id") or request.args.get("invoiceId"),
            source_document_id=request.args.get("source_document_id") or request.args.get("sourceDocumentId"),
            limit=_parse_limit_arg(100),
        )
    )


@app.route("/api/agent/vendor-communications/<communication_id>", methods=["GET"])
def api_agent_vendor_communication_detail(communication_id):
    return _agent_api_response(
        lambda: get_vendor_communication(communication_id),
        not_found_message="Vendor communication not found",
    )


@app.route("/api/agent/vendor-communications/draft", methods=["POST"])
def api_agent_vendor_communication_draft():
    def loader():
        data = request.get_json(force=True) or {}
        invoice_id = data.get("invoiceId") or data.get("invoice_id")
        review_reason = data.get("reviewReason") or data.get("review_reason") or "needs_clarification"
        source_document_id = data.get("sourceDocumentId") or data.get("source_document_id")
        if not invoice_id:
            raise ValueError("invoiceId is required")
        return create_vendor_communication_draft(
            invoice_id,
            review_reason=review_reason,
            source_document_id=source_document_id,
        )

    return _agent_mutation_response(loader, created=True)


@app.route("/api/agent/vendor-communications/<communication_id>/approve", methods=["POST"])
def api_agent_vendor_communication_approve(communication_id):
    def loader():
        data = request.get_json(force=True) or {}
        approved_by = (data.get("approvedBy") or data.get("approved_by") or "").strip()
        if not approved_by:
            raise ValueError("approvedBy is required")
        return approve_vendor_communication(communication_id, approved_by=approved_by)

    return _agent_mutation_response(loader)


@app.route("/api/agent/vendor-communications/<communication_id>/reject", methods=["POST"])
def api_agent_vendor_communication_reject(communication_id):
    def loader():
        data = request.get_json(force=True) or {}
        rejected_by = (data.get("rejectedBy") or data.get("rejected_by") or "").strip()
        if not rejected_by:
            raise ValueError("rejectedBy is required")
        return reject_vendor_communication(communication_id, rejected_by=rejected_by)

    return _agent_mutation_response(loader)


@app.route("/api/agent/vendor-communications/<communication_id>/mark-sent", methods=["POST"])
def api_agent_vendor_communication_mark_sent(communication_id):
    def loader():
        data = request.get_json(force=True) or {}
        sent_by = (data.get("sentBy") or data.get("sent_by") or "").strip()
        if not sent_by:
            raise ValueError("sentBy is required")
        return mark_vendor_communication_sent(communication_id, sent_by=sent_by)

    return _agent_mutation_response(loader)


@app.route("/api/agent/payments/authorizations", methods=["GET"])
def api_agent_payment_authorizations():
    return _agent_api_response(
        lambda: list_payment_authorizations(
            approval_status=request.args.get("approval_status") or request.args.get("approvalStatus"),
            risk_level=request.args.get("risk_level") or request.args.get("riskLevel"),
            limit=_parse_limit_arg(100),
        )
    )


@app.route("/api/agent/payments/authorizations/<request_id>", methods=["GET"])
def api_agent_payment_authorization_detail(request_id):
    return _agent_api_response(
        lambda: get_payment_authorization(request_id),
        not_found_message="Payment authorization request not found",
    )


@app.route("/api/agent/payments/authorize", methods=["POST"])
def api_agent_payment_authorize():
    def loader():
        data = request.get_json(force=True) or {}
        invoice_ids = data.get("invoiceIds") or data.get("invoice_ids") or []
        customer = data.get("customer") or {}
        currency = data.get("currency")
        save_method = bool(data.get("saveMethod") or data.get("save_method"))
        requested_by = data.get("requestedBy") or data.get("requested_by")
        return submit_payment_authorization_request(
            invoice_ids,
            customer,
            currency=currency,
            save_method=save_method,
            requested_by=requested_by,
        )

    return _agent_mutation_response(loader, created=True)


@app.route("/api/agent/payments/route", methods=["POST"])
def api_agent_payment_route():
    try:
        data = request.get_json(force=True) or {}
        invoice_ids = data.get("invoiceIds") or data.get("invoice_ids") or []
        customer = data.get("customer") or {}
        currency = data.get("currency")
        save_method = bool(data.get("saveMethod") or data.get("save_method"))
        requested_by = data.get("requestedBy") or data.get("requested_by")
        payload = submit_payment_route(
            invoice_ids,
            customer,
            currency=currency,
            save_method=save_method,
            requested_by=requested_by,
        )
        status_code = 202 if payload.get("status") == "pending_approval" else 200
        return jsonify(payload), status_code
    except Exception as e:
        default_status = 503 if isinstance(e, RuntimeError) else 400 if isinstance(e, ValueError) else 500
        return _error_response(e, default_status=default_status)


@app.route("/api/agent/payments/authorizations/<request_id>/approve", methods=["POST"])
def api_agent_payment_authorization_approve(request_id):
    def loader():
        data = request.get_json(force=True) or {}
        approved_by = (data.get("approvedBy") or data.get("approved_by") or "").strip()
        if not approved_by:
            raise ValueError("approvedBy is required")
        return approve_payment_authorization(request_id, approved_by=approved_by)

    return _agent_mutation_response(loader)


@app.route("/api/agent/payments/authorizations/<request_id>/reject", methods=["POST"])
def api_agent_payment_authorization_reject(request_id):
    def loader():
        data = request.get_json(force=True) or {}
        rejected_by = (data.get("rejectedBy") or data.get("rejected_by") or "").strip()
        if not rejected_by:
            raise ValueError("rejectedBy is required")
        return reject_payment_authorization(request_id, rejected_by=rejected_by)

    return _agent_mutation_response(loader)


@app.route("/api/agent/payments/authorizations/<request_id>/execute", methods=["POST"])
def api_agent_payment_authorization_execute(request_id):
    return _agent_mutation_response(lambda: submit_payment_execution(request_id))

# Vendor API Routes
@app.route("/api/vendors",methods=["GET"])
def api_vendors_list():
    """Get all vendors with detailed information"""
    try:
        vendors = get_all_vendors_detailed()
        return jsonify(vendors)
    except Exception as e:
        return _error_response(e)

@app.route("/api/vendors/stats",methods=["GET"])
def api_vendor_stats():
    """Get vendor summary statistics"""
    try:
        stats = get_vendor_stats()
        return jsonify(stats)
    except Exception as e:
        return _error_response(e)

@app.route("/api/vendors/<vendor_id>",methods=["GET"])
def api_vendor_detail(vendor_id):
    """Get detailed vendor information"""
    try:
        vendor = get_vendor_by_id_detailed(vendor_id)
        if vendor:
            return jsonify(vendor)
        else:
            return jsonify({"error": "Vendor not found"}), 404
    except Exception as e:
        return _error_response(e)

@app.route("/api/vendors", methods=["POST"])
def api_vendor_create():
    """Create a new vendor."""
    try:
        data = request.get_json(force=True) or {}
        name = (data.get("name") or "").strip()
        tax_id = data.get("taxId") or data.get("tax_id")
        contact = data.get("contact") or data.get("contact_info")
        address = data.get("address")
        if not name:
            return jsonify({"error": "name is required"}), 400
        vendor = create_vendor(name=name, tax_id=tax_id, contact_info=contact, address=address)
        publish_live_update("vendor.created", {"vendorId": str(vendor.get("id")) if isinstance(vendor, dict) else None})
        return jsonify(vendor), 201
    except Exception as e:
        return _error_response(e, default_status=400)

@app.route("/api/vendors/<vendor_id>", methods=["DELETE"])
def api_vendor_delete(vendor_id):
    """Delete a vendor and related records."""
    try:
        summary = delete_vendor(vendor_id)
        publish_live_update("vendor.deleted", {"vendorId": vendor_id})
        return jsonify({"deleted": summary})
    except Exception as e:
        return _error_response(e, default_status=400)

# Mentions (typeahead for @)
@app.route("/api/vendors/<vendor_id>/mentions", methods=["GET"])
def api_vendor_mentions(vendor_id):
    try:
        kind = (request.args.get("kind") or "").lower()
        q = (request.args.get("q") or "").strip()
        limit = int(request.args.get("limit", 10))
        with get_conn() as conn:
            with conn.cursor() as cur:
                items = []
                if kind == "pos":
                    cur.execute(
                        """
                        SELECT id, po_number, total_amount, currency
                        FROM purchase_orders
                        WHERE vendor_id=%s AND (%s='' OR po_number ILIKE %s)
                        ORDER BY created_at DESC NULLS LAST, id DESC
                        LIMIT %s
                        """,
                        (vendor_id, q, f"%{q}%", limit)
                    )
                    for row in cur.fetchall():
                        items.append({
                            "id": str(row[0]),
                            "label": row[1] or str(row[0]),
                            "meta": {
                                "amount": float(row[2]) if row[2] else 0.0,
                                "currency": row[3] or "USD",
                            }
                        })
                else:
                    cur.execute(
                        """
                        SELECT id, invoice_number, total_amount, currency, invoice_date
                        FROM invoices
                        WHERE vendor_id=%s AND (%s='' OR invoice_number ILIKE %s)
                        ORDER BY created_at DESC NULLS LAST, id DESC
                        LIMIT %s
                        """,
                        (vendor_id, q, f"%{q}%", limit)
                    )
                    for row in cur.fetchall():
                        items.append({
                            "id": str(row[0]),
                            "label": row[1] or str(row[0]),
                            "meta": {
                                "amount": float(row[2]) if row[2] else 0.0,
                                "currency": row[3] or "USD",
                                "date": row[4].isoformat() if row[4] else None,
                            }
                        })
        return jsonify({"items": items})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

# Chat lifecycle
@app.route("/api/vendors/<vendor_id>/chat/start", methods=["POST"])
def api_chat_start(vendor_id):
    try:
        payload = (request.json or {}) if request.is_json else {}
        title = payload.get("title")
        reuse = payload.get("reuseLatest", True)
        chat_id = None
        if reuse:
            chats = db_list_chats(vendor_id, limit=1)
            if chats:
                chat_id = chats[0]["id"]
        if not chat_id:
            chat_id = db_create_chat(vendor_id, title=title)
        return jsonify({"chatId": chat_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route("/api/vendors/<vendor_id>/chat", methods=["GET"])
def api_chat_list(vendor_id):
    try:
        limit = int(request.args.get("limit", 20))
        items = db_list_chats(vendor_id, limit=limit)
        return jsonify({"chats": items})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route("/api/vendors/<vendor_id>/chat/<chat_id>/messages", methods=["GET"])
def api_chat_messages_list(vendor_id, chat_id):
    try:
        # enforce vendor scoping
        v = get_chat_vendor(chat_id)
        if v != vendor_id:
            return jsonify({"error": "chat does not belong to vendor"}), 403
        limit = int(request.args.get("limit", 50))
        before = request.args.get("before")
        items = db_list_messages(chat_id, limit=limit, before=before)
        return jsonify({"messages": items})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route("/api/vendors/<vendor_id>/chat/<chat_id>/messages", methods=["POST"])
def api_chat_send(vendor_id, chat_id):
    try:
        v = get_chat_vendor(chat_id)
        if v != vendor_id:
            return jsonify({"error": "chat does not belong to vendor"}), 403
        data = request.get_json(force=True) or {}
        prompt = data.get("prompt") or ""
        tags = data.get("tags") or {}
        stream = bool(data.get("stream"))
        inv_ids = [i for i in (tags.get("invoices") or []) if i]
        po_ids = [p for p in (tags.get("pos") or []) if p]
        # Always store user message
        db_add_message(chat_id, "user", prompt, tags={"invoices": inv_ids, "pos": po_ids})

        if stream or (request.headers.get('Accept') == 'text/event-stream'):
            # Stream inline (same logic as /stream, but using local prompt/tags)
            def generator():
                full = []
                try:
                    from chat_llm import _get_claude_client, _build_system_prompt, _summarize_invoice as _si, _summarize_po as _sp
                    from vendor_db import get_vendor_by_id_detailed as _get_vendor
                    from invoice_db import get_invoice_by_id as _get_inv
                    from po_db import get_po_by_id as _get_po
                    import json as _json
                    vendor = _get_vendor(vendor_id)
                    system_prompt = _build_system_prompt(vendor or {})
                    client = _get_claude_client()
                    if client is not None:
                        ctx = {"vendor": {"id": vendor_id, "name": (vendor or {}).get("name", "")}, "invoices": [], "pos": []}
                        for _iid in inv_ids or []:
                            try:
                                _inv = _get_inv(_iid)
                                if _inv:
                                    ctx["invoices"].append(_si(_inv))
                            except Exception:
                                pass
                        for _pid in po_ids or []:
                            try:
                                _po = _get_po(_pid)
                                if _po:
                                    ctx["pos"].append(_sp(_po))
                            except Exception:
                                pass
                        user_text = (
                            "Context JSON (use strictly, do not fabricate outside it):\n"
                            + _json.dumps(ctx, ensure_ascii=False)
                            + "\n\nUser Question:\n"
                            + prompt
                        )
                        model = os.getenv("CLAUDE_MODEL", "claude-3-7-sonnet-20250219")
                        with client.messages.stream(
                            model=model,
                            max_tokens=1024,
                            system=system_prompt,
                            messages=[{"role": "user", "content": [{"type": "text", "text": user_text}]}],
                        ) as stream:
                            for event in stream:
                                try:
                                    if getattr(event, "type", "") == "content_block_delta":
                                        delta = getattr(event, "delta", None)
                                        if delta and getattr(delta, "type", "") == "text_delta":
                                            chunk = getattr(delta, "text", "")
                                            if chunk:
                                                full.append(chunk)
                                                yield f"data: {chunk}\n\n"
                                except Exception:
                                    pass
                            # finalize
                            final = stream.get_final_message()
                            final_text_parts = []
                            try:
                                for block in getattr(final, "content", []) or []:
                                    if getattr(block, "type", "") == "text":
                                        final_text_parts.append(getattr(block, "text", ""))
                            except Exception:
                                pass
                            final_text = "".join(final_text_parts)
                            if final_text and (not full or final_text != "".join(full)):
                                extra = final_text[len("".join(full)) :]
                                if extra:
                                    yield f"data: {extra}\n\n"
                            # persist assistant + title
                            try:
                                msg_text = "".join(full) or final_text
                                db_add_message(chat_id, "assistant", msg_text, tags={"invoices": inv_ids, "pos": po_ids})
                                title = generate_chat_title(vendor or {}, prompt, ctx.get("invoices") or [], ctx.get("pos") or [])
                                from chat_db import update_chat_title
                                update_chat_title(chat_id, title)
                            except Exception:
                                pass
                            return
                except Exception:
                    pass

                # Fallback to non-streaming chunking
                text = generate_vendor_response(vendor_id, prompt, invoice_ids=inv_ids or None, po_ids=po_ids or None)
                for i in range(0, len(text), 80):
                    chunk = text[i:i+80]
                    full.append(chunk)
                    yield f"data: {chunk}\n\n"
                try:
                    db_add_message(chat_id, "assistant", "".join(full), tags={"invoices": inv_ids, "pos": po_ids})
                except Exception:
                    pass

            return Response(stream_with_context(generator()), mimetype='text/event-stream')

        # Non-streaming path
        reply = generate_vendor_response(vendor_id, prompt, invoice_ids=inv_ids or None, po_ids=po_ids or None)
        db_add_message(chat_id, "assistant", reply, tags={"invoices": inv_ids, "pos": po_ids})

        # Try to assign a title if missing
        try:
            from vendor_db import get_vendor_by_id_detailed as _get_vendor
            from invoice_db import get_invoice_by_id as _get_inv
            from po_db import get_po_by_id as _get_po
            vendor = _get_vendor(vendor_id) or {}
            invs = []
            pos_list = []
            for _iid in inv_ids:
                try:
                    ii = _get_inv(_iid)
                    if ii: invs.append(ii)
                except Exception:
                    pass
            for _pid in po_ids:
                try:
                    pp = _get_po(_pid)
                    if pp: pos_list.append(pp)
                except Exception:
                    pass
            title = generate_chat_title(vendor, prompt, invs, pos_list)
            from chat_db import update_chat_title
            update_chat_title(chat_id, title)
        except Exception:
            pass

        return jsonify({"reply": reply})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route("/api/vendors/<vendor_id>/chat/<chat_id>/stream", methods=["POST","GET"])
def api_chat_stream(vendor_id, chat_id):
    """Stream assistant reply token-by-token (SSE-like)."""
    try:
        v = get_chat_vendor(chat_id)
        if v != vendor_id:
            return jsonify({"error": "chat does not belong to vendor"}), 403
        if request.method == "GET":
            prompt = request.args.get("prompt", "")
            inv_ids = request.args.getlist("inv")
            po_ids = request.args.getlist("po")
        else:
            data = request.get_json(force=True) or {}
            prompt = data.get("prompt") or ""
            tags = data.get("tags") or {}
            inv_ids = [i for i in (tags.get("invoices") or []) if i]
            po_ids = [p for p in (tags.get("pos") or []) if p]

        # store user message immediately
        db_add_message(chat_id, "user", prompt, tags={"invoices": inv_ids, "pos": po_ids})

        def generate():
            full = []
            # Try Claude streaming if configured
            try:
                from chat_llm import _get_claude_client, _build_system_prompt, _summarize_invoice as _si, _summarize_po as _sp
                from vendor_db import get_vendor_by_id_detailed as _get_vendor
                from invoice_db import get_invoice_by_id as _get_inv
                from po_db import get_po_by_id as _get_po
                import json as _json
                vendor = _get_vendor(vendor_id)
                if vendor:
                    system_prompt = _build_system_prompt(vendor)
                else:
                    system_prompt = "You are a vendor-scoped assistant."
                client = _get_claude_client()
                if client is not None:
                    # assemble context with tagged entities
                    ctx = {
                        "vendor": {
                            "id": vendor.get("id") if vendor else vendor_id,
                            "name": vendor.get("name") if vendor else "",
                        },
                        "invoices": [],
                        "pos": [],
                    }
                    for _iid in inv_ids or []:
                        try:
                            _inv = _get_inv(_iid)
                            if _inv:
                                ctx["invoices"].append(_si(_inv))
                        except Exception:
                            pass
                    for _pid in po_ids or []:
                        try:
                            _po = _get_po(_pid)
                            if _po:
                                ctx["pos"].append(_sp(_po))
                        except Exception:
                            pass
                    user_text = (
                        "Context JSON (use strictly, do not fabricate outside it):\n"
                        + _json.dumps(ctx, ensure_ascii=False)
                        + "\n\nUser Question:\n"
                        + prompt
                    )
                    model = os.getenv("CLAUDE_MODEL", "claude-3-7-sonnet-20250219")
                    with client.messages.stream(
                        model=model,
                        max_tokens=1024,
                        system=system_prompt,
                        messages=[{"role": "user", "content": [{"type": "text", "text": user_text}]}],
                    ) as stream:
                        for event in stream:
                            try:
                                if getattr(event, "type", "") == "content_block_delta":
                                    delta = getattr(event, "delta", None)
                                    if delta and getattr(delta, "type", "") == "text_delta":
                                        chunk = getattr(delta, "text", "")
                                        if chunk:
                                            full.append(chunk)
                                            yield f"data: {chunk}\n\n"
                            except Exception:
                                pass
                        final = stream.get_final_message()
                        final_text_parts = []
                        try:
                            for block in getattr(final, "content", []) or []:
                                if getattr(block, "type", "") == "text":
                                    final_text_parts.append(getattr(block, "text", ""))
                        except Exception:
                            pass
                        final_text = "".join(final_text_parts)
                        if final_text and (not full or final_text != "".join(full)):
                            # Append any trailing content not streamed
                            extra = final_text[len("".join(full)) :]
                            if extra:
                                yield f"data: {extra}\n\n"
                        # persist assistant message & set title if missing
                        try:
                            msg_text = "".join(full) or final_text
                            db_add_message(chat_id, "assistant", msg_text, tags={"invoices": inv_ids, "pos": po_ids})
                            # title
                            from chat_db import update_chat_title
                            title = generate_chat_title(vendor or {}, prompt, ctx.get("invoices") or [], ctx.get("pos") or [])
                            update_chat_title(chat_id, title)
                        except Exception:
                            pass
                        return
            except Exception:
                pass

            # Fallback: non-streaming generation chunked
            text = generate_vendor_response(vendor_id, prompt, invoice_ids=inv_ids or None, po_ids=po_ids or None)
            for i in range(0, len(text), 80):
                chunk = text[i:i+80]
                full.append(chunk)
                yield f"data: {chunk}\n\n"
            try:
                db_add_message(chat_id, "assistant", "".join(full), tags={"invoices": inv_ids, "pos": po_ids})
            except Exception:
                pass

        headers = {
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        }
        return Response(stream_with_context(generate()), headers=headers)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route("/api/invoices/exceptions", methods=["GET"])
def api_exception_invoices():
    """Get exception invoices (unmatched, vendor_mismatch, needs_review)"""
    try:
        vendor_id = request.args.get("vendor_id") or request.args.get("vendorId")
        limit = request.args.get("limit", default=100, type=int)
        status = request.args.get("status")
        items = get_exception_invoices(vendor_id=vendor_id, limit=limit, status=status)
        return jsonify(items)
    except Exception as e:
        return _error_response(e)

# Purchase Order API Route
@app.route("/api/purchase-orders/<po_id>", methods=["GET"])
def api_purchase_order_detail(po_id):
    """Get detailed purchase order information"""
    try:
        po = get_po_detail(po_id)
        if po:
            return jsonify(po)
        else:
            return jsonify({"error": "Purchase order not found"}), 404
    except Exception as e:
        return _error_response(e)

# Payable invoices
@app.route("/api/invoices/payable", methods=["GET"])
def api_invoices_payable():
    try:
        vendor_id = request.args.get("vendor_id") or request.args.get("vendorId")
        currency = request.args.get("currency")
        limit = request.args.get("limit", default=200, type=int)
        items = get_payable_invoices(vendor_id=vendor_id, currency=currency, limit=limit)
        return jsonify(items)
    except Exception as e:
        return _error_response(e)

# Create PaymentIntent
@app.route("/api/payments/create-intent", methods=["POST"])
def api_create_payment_intent():
    try:
        data = request.get_json(force=True)
        invoice_ids = data.get("invoiceIds") or []
        customer = data.get("customer") or {}
        currency = data.get("currency")
        save_method = bool(data.get("saveMethod"))
        result = create_payment_intent_for_invoices(invoice_ids, customer, currency, save_method)
        return jsonify(result)
    except Exception as e:
        return _error_response(e, default_status=400)

@app.route("/api/payments/pending-confirmations", methods=["GET"])
def api_pending_payment_confirmations():
    try:
        limit = int(request.args.get("limit", "25"))
        return jsonify(list_pending_payment_confirmations(limit=limit))
    except Exception as e:
        return _error_response(e, default_status=400)

@app.route("/api/payments/history", methods=["GET"])
def api_payment_history():
    try:
        limit = int(request.args.get("limit", "25"))
        vendor_id = request.args.get("vendor_id")
        currency = request.args.get("currency")
        return jsonify(list_payment_history(limit=limit, vendor_id=vendor_id, currency=currency))
    except Exception as e:
        return _error_response(e, default_status=400)

@app.route("/api/payments/confirm", methods=["POST"])
def api_confirm_payment():
    """Client-driven confirmation to finalize payment without webhooks.
       Verifies with Stripe and marks invoices paid if succeeded.
    """
    try:
        data = request.get_json(force=True)
        pi_id = data.get("paymentIntentId")
        if not pi_id:
            return jsonify({"error": "paymentIntentId is required"}), 400
        result = confirm_payment_intent(pi_id)
        return jsonify(result)
    except Exception as e:
        return _error_response(e, default_status=400)

@app.route("/api/payments/cancel", methods=["POST"])
def api_cancel_payment():
    """Revert invoices to previous status for a failed/canceled payment intent (no webhook)."""
    try:
        data = request.get_json(force=True)
        pi_id = data.get("paymentIntentId")
        if not pi_id:
            return jsonify({"error": "paymentIntentId is required"}), 400
        mark_payment_failed_or_canceled(pi_id)
        return jsonify({"status": "reverted"})
    except Exception as e:
        return _error_response(e, default_status=400)

def start_scheduler():
    scheduler=BackgroundScheduler(daemon=True)
    scheduler.add_job(run_job,"interval",seconds=CHECK_INTERVAL_SECONDS)
    scheduler.add_job(
        lambda: run_sla_monitor_once(triggered_by="scheduler"),
        "interval",
        seconds=max(5, int(os.getenv("SLA_MONITOR_INTERVAL_SECONDS", "60"))),
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()
    start_agent_worker_thread()

if __name__=="__main__":
    start_scheduler()
    app.run(host="127.0.0.1",port=int(os.getenv("PORT","5000")),debug=False)
