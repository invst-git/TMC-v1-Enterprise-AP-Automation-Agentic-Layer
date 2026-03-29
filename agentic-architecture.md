# TMC-Agent: Multi-Agent Procure-to-Pay Architecture
**Upgrade design for TMC-v1 → Fully Agentic AP System**

---

## Executive Summary

TMC-v1 is already structured around discrete operational capabilities — intake, extraction, matching, exceptions, payments. This is the ideal foundation for agentification. The upgrade converts each workflow stage into a **tool-calling agent** with its own decision authority, failure recovery logic, and audit trail. The result is a P2P (Procure-to-Pay) multi-agent system that completes 85–90% of invoice lifecycles without human involvement, escalating only genuinely ambiguous cases with full reasoning context.

**Evaluation criteria met:**
| Criterion | How TMC-Agent achieves it |
|---|---|
| Depth of autonomy | 7 specialized agents, full P2P lifecycle, human touchpoints only at configurable confidence thresholds |
| Error recovery | Per-agent retry logic, alternative resolution paths, dead-letter escalation with context packets |
| Auditability | Every agent decision logged with inputs, outputs, confidence, and reasoning — queryable per invoice |
| Real-world applicability | Built directly on production AP workflows (OCR, PO matching, Stripe payments) |

---

## Agent Roster

### 1. Orchestrator Agent
**Role:** Central coordinator. Owns the workflow state machine. Plans execution, delegates to specialist agents, handles timeouts and re-routing.

**Tools:**
- `get_workflow_state(invoice_id)` → current state, SLA deadline, assigned agent
- `update_workflow_state(invoice_id, new_state, reason, confidence)` → transitions state machine
- `dispatch_task(agent_type, task_type, payload, priority)` → puts work onto task queue
- `query_stalled_tasks(max_age_minutes)` → finds stuck work items
- `escalate_to_human(invoice_id, reason, priority, review_packet)` → creates human review item
- `get_pending_count_by_agent()` → load-balancing signal

**Decision logic:** The Orchestrator is the only agent with a system-level view. It resolves conflicts between agent outputs, enforces the state machine transitions, and decides when confidence is too low for automation to proceed. It runs Claude with a structured tool-calling loop.

**Self-correction:** If a delegated task times out, the Orchestrator re-queues it (up to `MAX_RETRIES`) before escalating. It detects cyclical failure patterns (e.g. OCR repeatedly failing on same file) and reroutes rather than retrying.

---

### 2. Intake Agent
**Role:** Replaces the current `email_client.py` + `document_pipeline.py` with an agent that reasons about documents before committing them.

**Tools (new/upgraded):**
- `poll_inbox(lookback_minutes)` → returns list of candidate attachments
- `classify_document(file_path)` → LLM-based: is this actually an invoice? probability score
- `detect_multi_invoice_pdf(file_path)` → upgraded `pdf_segmentation.py` with LLM boundary reasoning
- `extract_sender_signals(email_metadata)` → infer vendor from email domain, signature
- `quarantine_document(file_path, reason)` → moves to quarantine, logs reason
- `route_document(file_path, vendor_signals, confidence)` → vendor pre-association attempt

**Upgrade over current:** The current `is_invoice_attachment()` is a file-extension/MIME filter. The Intake Agent uses LLM classification to reject non-invoices (remittance advices, delivery notes, statements) before they enter OCR. Reduces false positives entering the pipeline.

**Self-correction:** If a document is quarantined, the agent emails the sender (via Vendor Communication Agent) requesting a clean re-submission, rather than silently discarding.

---

### 3. Extraction & Validation Agent
**Role:** Replaces passive `ocr_landingai.py` + `invoice_db.py` with an agent that validates and scores what it extracts.

**Tools (new/upgraded):**
- `run_ocr(file_path, focus_hints)` → calls Landing AI, returns fields + confidence per field
- `score_extraction_quality(fields)` → aggregate confidence; flags low-confidence fields by name
- `validate_field_constraints(fields)` → date logic, amount consistency, required fields present
- `detect_anomalies(fields, vendor_id)` → cross-reference against vendor's invoice history: outlier amounts, duplicate invoice numbers, unusual line items
- `request_reextraction(file_path, failed_fields, hint_prompt)` → retry OCR with targeted prompt
- `persist_invoice(validated_fields, vendor_id, source_file)` → writes to DB

**Upgrade over current:** The current system writes whatever OCR returns. The Extraction Agent iterates: low-confidence fields trigger a targeted re-extraction pass with explicit hints before the invoice is persisted. Duplicate invoice number detection happens here, not downstream.

**Self-correction:** Up to 2 re-extraction attempts with progressively stronger hints. If confidence is still below threshold after retries → invoice is persisted with `needs_review` status and a structured review packet (which specific fields failed, raw OCR output, suggested corrections).

---

### 4. PO Matching Agent
**Role:** Replaces deterministic `po_matching.py` with a reasoning-capable agent that handles partial matches, split POs, and tolerance edge cases.

**Tools (new/upgraded):**
- `search_purchase_orders(vendor_id, po_number, currency, amount_range)` → returns candidates
- `compute_match_score(invoice, po_candidate)` → multi-factor score: PO number similarity, amount delta, line item overlap, currency, vendor
- `explain_mismatch(invoice, po_candidate)` → LLM-generated plain-English reason for why a near-miss didn't match
- `detect_split_po(invoice_lines, po_lines)` → checks if invoice covers a subset of a partial-receipt PO
- `query_vendor_po_history(vendor_id, lookback_days)` → pattern context for matching heuristics
- `flag_for_manual_match(invoice_id, top_candidates, scores, explanations)` → structured escalation

**Upgrade over current:** The current matcher does exact PO number lookup + single amount tolerance check. The PO Matching Agent runs a scored search across multiple POs, handles invoices that partially fulfill a PO, and when a match fails, produces an explanation (e.g. "PO-2041 was found but amount exceeds PO value by 12% — possible change order not reflected in system") rather than a bare `unmatched` status.

**Self-correction:** On amount mismatch, the agent checks if a change order or PO amendment exists. On PO number not found, it applies fuzzy matching against recent open POs for the same vendor before giving up.

---

### 5. Exception Resolution Agent
**Role:** The most novel agent. Autonomously investigates and resolves exceptions rather than routing them all to humans.

**Tools (new):**
- `analyze_exception_root_cause(invoice_id)` → categorizes: vendor_mismatch / no_po / amount_discrepancy / duplicate / data_quality / policy_violation
- `attempt_vendor_resolution(supplier_name, tax_id, email_domain)` → fuzzy vendor lookup + auto-link proposal
- `attempt_po_resolution(invoice_id)` → runs PO Matching Agent with relaxed tolerances + LLM reasoning
- `request_vendor_clarification(vendor_id, invoice_id, specific_questions)` → triggers outbound email via Vendor Communication Agent
- `propose_manual_resolution(invoice_id, resolution_type, confidence, reasoning)` → awaits human approval
- `auto_resolve(invoice_id, resolution_action)` → applies resolution when confidence > threshold
- `create_review_packet(invoice_id)` → structured JSON with: exception category, root cause, evidence, resolution options with pros/cons, recommended action

**Resolution capability matrix:**
| Exception Type | Auto-Resolution Capability | Threshold |
|---|---|---|
| Vendor mismatch (fuzzy name match found) | Yes, propose + auto-apply | confidence > 0.90 |
| Vendor mismatch (no candidate) | No — request vendor clarification | — |
| Amount discrepancy < 5% | Yes — flag for fast approval | 1-click human approval |
| Amount discrepancy 5–20% | Request vendor credit/debit memo | — |
| No PO found (open PO exists in window) | Yes — propose PO link, auto-apply | confidence > 0.85 |
| Duplicate invoice | Auto-reject with vendor notification | always |
| Data quality (low OCR confidence) | Trigger re-extraction | — |

**Self-correction:** If auto-resolution is applied and a human later overrides it, the agent logs the override and updates its confidence thresholds for that resolution type going forward (feedback loop).

---

### 6. SLA & Workflow Health Monitor Agent
**Role:** Continuous background agent. Detects process drift, predicts bottlenecks, re-queues stalled tasks, and alerts before SLA breaches.

**Tools (new):**
- `compute_invoice_age(invoice_id)` → age by status transition timestamps
- `check_sla_status(invoice_id)` → `ok / warning / breaching / breached` with time remaining
- `predict_breach_risk(invoice_id)` → based on historical processing times per status + current backlog
- `detect_process_drift()` → compares current throughput/error rates against 30-day baseline
- `requeue_stalled(invoice_id, agent_type)` → re-dispatches stuck items
- `generate_health_report(period)` → agent-level throughput, error rates, SLA compliance, escalation rates
- `trigger_alert(severity, message, invoice_ids)` → webhook / email to configured ops channel

**SLA configuration table (new `sla_configs` table):**
| From status | To status | Warning (hrs) | Breach (hrs) |
|---|---|---|---|
| `uploaded` | `extracted` | 0.5 | 2 |
| `extracted` | `matched_auto` or `unmatched` | 1 | 4 |
| `unmatched` | `needs_review` or resolved | 4 | 24 |
| `needs_review` | human action | 4 | 24 |
| `matched_auto` | `payment_pending` | 24 | 72 |
| `payment_pending` | `paid` | 4 | 24 |

**Self-correction:** The SLA agent re-queues stalled tasks (no status update in > warning threshold) automatically. It also detects if a specific agent is causing disproportionate delays and alerts ops.

---

### 7. Payment Authorization Agent
**Role:** Upgrades the manual payment flow with pre-payment validation, risk scoring, and configurable approval gates.

**Tools (new/upgraded):**
- `validate_payment_eligibility(invoice_ids)` → status check, currency consistency, vendor active check
- `score_fraud_risk(payment_batch)` → signals: unusual amount, new vendor, first invoice from sender, amount vs PO variance
- `check_approval_required(payment_batch, risk_score)` → policy-driven: below threshold = auto, above = human approval
- `request_approval(payment_id, approver_email, amount, risk_summary)` → sends structured approval request
- `execute_payment(payment_id)` → calls existing Stripe PaymentIntent flow
- `log_payment_decision(payment_id, decision, reasoning, risk_score)` → audit trail
- `reverse_payment(payment_id, reason)` → calls existing cancel flow with reason logged

**Upgrade over current:** Current flow requires a human to manually select invoices and click pay. Payment Agent can batch eligible invoices on a schedule (daily/weekly run), auto-execute low-risk batches, and route high-risk batches to an approver with a structured summary.

---

### 8. Vendor Communication Agent
**Role:** New agent. Handles outbound communication to vendors when data is missing or clarification is needed.

**Tools (new):**
- `draft_clarification_email(vendor_id, invoice_id, missing_fields, questions)` → LLM-drafted email
- `send_vendor_email(vendor_email, subject, body, invoice_id)` → SMTP send + logging
- `track_response(vendor_id, invoice_id, sent_at)` → monitors for reply
- `escalate_no_response(vendor_id, invoice_id, days_waited)` → triggers human follow-up

**Use cases activated:**
- Invoice number missing → email vendor requesting invoice reference
- Amount doesn't match PO → email vendor requesting credit memo or explanation
- Duplicate invoice detected → email vendor with duplicate notification
- OCR failed (unreadable scan) → email vendor requesting clean re-send

---

### 9. Audit Trail Agent (Cross-cutting)
**Role:** Not a workflow agent — a logging/reporting layer called by every other agent. Every decision written here.

**Tools (new):**
- `log_decision(agent, action, inputs, outputs, confidence, reasoning, invoice_id)` → writes to `agent_decisions`
- `query_decision_history(invoice_id)` → full audit trail for one invoice
- `generate_audit_report(invoice_id)` → human-readable chronological narrative
- `flag_decision(decision_id, reason)` → marks a decision for compliance review
- `compute_automation_rate(period)` → % of invoices fully auto-processed

---

## New Database Schema

```sql
-- Agent task queue (durable, replaces APScheduler job)
CREATE TABLE agent_tasks (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  agent_type   VARCHAR(50)  NOT NULL,
  task_type    VARCHAR(100) NOT NULL,
  payload      JSONB        NOT NULL DEFAULT '{}',
  status       VARCHAR(20)  NOT NULL DEFAULT 'queued',
  -- queued | in_progress | completed | failed | escalated | dead_letter
  priority     INT          NOT NULL DEFAULT 5,
  parent_id    UUID         REFERENCES agent_tasks(id),
  invoice_id   INT          REFERENCES invoices(id),
  retry_count  INT          NOT NULL DEFAULT 0,
  max_retries  INT          NOT NULL DEFAULT 3,
  error_msg    TEXT,
  created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  started_at   TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  next_retry_at TIMESTAMPTZ
);
CREATE INDEX idx_tasks_status_agent ON agent_tasks(status, agent_type);
CREATE INDEX idx_tasks_invoice ON agent_tasks(invoice_id);

-- Decision audit log (immutable)
CREATE TABLE agent_decisions (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  task_id       UUID         REFERENCES agent_tasks(id),
  invoice_id    INT          REFERENCES invoices(id),
  agent_type    VARCHAR(50)  NOT NULL,
  action_taken  VARCHAR(200) NOT NULL,
  inputs        JSONB        NOT NULL,
  outputs       JSONB        NOT NULL,
  confidence    FLOAT,
  reasoning     TEXT,
  was_overridden BOOLEAN     NOT NULL DEFAULT FALSE,
  override_by   VARCHAR(200),
  override_reason TEXT,
  created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_decisions_invoice ON agent_decisions(invoice_id);
CREATE INDEX idx_decisions_agent ON agent_decisions(agent_type, created_at);

-- Workflow state machine
CREATE TABLE workflow_states (
  id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  invoice_id     INT          NOT NULL UNIQUE REFERENCES invoices(id),
  current_state  VARCHAR(50)  NOT NULL,
  previous_state VARCHAR(50),
  state_data     JSONB        NOT NULL DEFAULT '{}',
  assigned_agent VARCHAR(50),
  sla_deadline   TIMESTAMPTZ,
  breach_risk    VARCHAR(20), -- ok | warning | breaching | breached
  entered_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  updated_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- SLA configuration
CREATE TABLE sla_configs (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  from_status         VARCHAR(50)  NOT NULL,
  to_status           VARCHAR(50)  NOT NULL,
  warning_hours       INT          NOT NULL,
  breach_hours        INT          NOT NULL,
  priority_multiplier FLOAT        NOT NULL DEFAULT 1.0,
  active              BOOLEAN      NOT NULL DEFAULT TRUE
);

-- Human review queue
CREATE TABLE human_review_queue (
  id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  invoice_id     INT          NOT NULL REFERENCES invoices(id),
  exception_type VARCHAR(100) NOT NULL,
  priority       VARCHAR(20)  NOT NULL DEFAULT 'medium',
  -- low | medium | high | urgent
  review_packet  JSONB        NOT NULL,
  -- structured: root_cause, evidence, resolution_options, recommended_action
  created_by     VARCHAR(50)  NOT NULL,
  assigned_to    VARCHAR(200),
  status         VARCHAR(20)  NOT NULL DEFAULT 'pending',
  -- pending | in_review | resolved | rejected
  resolution     TEXT,
  created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  resolved_at    TIMESTAMPTZ
);

-- Vendor communications log
CREATE TABLE vendor_communications (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  vendor_id   INT          REFERENCES vendors(id),
  invoice_id  INT          REFERENCES invoices(id),
  direction   VARCHAR(10)  NOT NULL, -- outbound | inbound
  channel     VARCHAR(20)  NOT NULL DEFAULT 'email',
  subject     TEXT,
  body        TEXT,
  status      VARCHAR(20)  NOT NULL DEFAULT 'sent',
  -- sent | delivered | replied | bounced | no_response
  sent_at     TIMESTAMPTZ,
  replied_at  TIMESTAMPTZ,
  created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
```

---

## Agent Runtime Architecture

### Task Queue Loop
```
┌─────────────────────────────────────────────┐
│              Orchestrator Agent              │
│  Claude tool-calling loop, 60s polling       │
│                                             │
│  1. get_pending_tasks()                     │
│  2. For each task: dispatch to agent         │
│  3. Await result or timeout                 │
│  4. Log decision via Audit Trail Agent       │
│  5. update_workflow_state()                 │
│  6. If retry_count > max_retries → DLQ      │
└─────────────────────────────────────────────┘
```

### Tool-Calling Implementation
Each agent is implemented as a Claude API call with a structured tool set:

```python
# agent_runtime.py (new)
class AgentRuntime:
    def __init__(self, agent_type: str, tools: list[dict]):
        self.agent_type = agent_type
        self.tools = tools
        self.client = anthropic.Anthropic()
    
    def run(self, task: AgentTask) -> AgentResult:
        messages = [{"role": "user", "content": task.to_prompt()}]
        
        while True:
            response = self.client.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=4096,
                tools=self.tools,
                system=self.get_system_prompt(),
                messages=messages,
            )
            
            # Log every tool use to audit trail
            for block in response.content:
                if block.type == "tool_use":
                    audit_log(self.agent_type, block.name, block.input)
            
            if response.stop_reason == "end_turn":
                return AgentResult.from_response(response)
            
            # Execute tool calls, append results
            tool_results = self.execute_tools(response.content)
            messages += [
                {"role": "assistant", "content": response.content},
                {"role": "user", "content": tool_results},
            ]
```

### Error Recovery Hierarchy
```
Level 1: Agent retry with modified parameters (up to MAX_RETRIES)
Level 2: Alternative resolution path (e.g. fuzzy match after exact match fails)
Level 3: Sibling agent delegation (Exception Agent handles what Matching Agent couldn't)
Level 4: Human review queue with structured packet (full context, recommended action)
Level 5: Dead Letter Queue (failed after all recovery attempts; ops alert triggered)
```

---

## Autonomy Assessment

### Invoice lifecycle automation rate (target)

| Scenario | Human touchpoints | Auto-rate |
|---|---|---|
| Clean invoice, PO match within tolerance | 0 | 100% |
| Clean invoice, PO match with small variance | 0 (auto-resolve) | 100% |
| Vendor mismatch, fuzzy name match found | 0 (confidence > 0.90) | 100% |
| Low OCR confidence, re-extraction succeeds | 0 | 100% |
| Amount discrepancy 5–20% | 1 (fast approval) | ~90% |
| No PO found, open PO exists | 0 (agent proposes + auto-links) | ~85% |
| New vendor, no history | 1 (vendor creation approval) | ~70% |
| Duplicate invoice | 0 (auto-reject + vendor notified) | 100% |
| Genuine exception (fraud signal, policy breach) | 1 (intentional gate) | — |

**Overall target: ~85% fully automated, ~12% fast-approval (1-click), ~3% full human review**

---

## Audit Trail Design

Every invoice generates a queryable narrative:

```json
{
  "invoice_id": 4821,
  "audit_trail": [
    {
      "timestamp": "2026-03-27T09:14:02Z",
      "agent": "intake_agent",
      "action": "classify_document",
      "confidence": 0.97,
      "reasoning": "Document contains invoice number pattern, total amount field, and payment terms. Classified as invoice.",
      "inputs": {"file": "acme_inv_march.pdf"},
      "outputs": {"classification": "invoice", "vendor_signal": "acme@acme.com"}
    },
    {
      "timestamp": "2026-03-27T09:14:18Z",
      "agent": "extraction_agent",
      "action": "run_ocr",
      "confidence": 0.91,
      "reasoning": "All required fields extracted. Invoice total field confidence 0.78 — triggered re-extraction.",
      "inputs": {"file": "acme_inv_march.pdf"},
      "outputs": {"invoice_number": "INV-2041", "total": 15420.00, "low_confidence_fields": ["total"]}
    },
    {
      "timestamp": "2026-03-27T09:14:35Z",
      "agent": "extraction_agent",
      "action": "request_reextraction",
      "confidence": 0.96,
      "reasoning": "Re-extraction with total amount focus hint raised confidence from 0.78 to 0.96.",
      "inputs": {"focus": "total_amount_field"},
      "outputs": {"total": 15420.00, "confidence": 0.96}
    },
    {
      "timestamp": "2026-03-27T09:14:52Z",
      "agent": "po_matching_agent",
      "action": "compute_match_score",
      "confidence": 0.98,
      "reasoning": "PO-2041 found. Amount delta $0.00. Currency match. Vendor match. Marking matched_auto.",
      "inputs": {"invoice_id": 4821, "po_candidates": ["PO-2041"]},
      "outputs": {"matched_po": "PO-2041", "status": "matched_auto"}
    },
    {
      "timestamp": "2026-03-27T09:15:10Z",
      "agent": "sla_monitor",
      "action": "check_sla_status",
      "confidence": 1.0,
      "reasoning": "Invoice processed in 68 seconds. SLA: 2 hours. Status: OK.",
      "outputs": {"sla_status": "ok", "time_to_breach_hours": 1.98}
    }
  ]
}
```

---

## Build Roadmap

### Phase 1 — Agent Backbone 
- `agent_tasks`, `agent_decisions`, `workflow_states`, `sla_configs` tables
- `AgentRuntime` base class with tool-calling loop
- Orchestrator Agent (workflow state machine)
- Audit Trail Agent (logging layer)
- Replace `APScheduler` jobs with durable task queue

### Phase 2 — Intelligent Processing 
- Upgrade Intake Agent (LLM document classification)
- Upgrade Extraction & Validation Agent (confidence scoring, re-extraction)
- Upgrade PO Matching Agent (multi-factor scoring, mismatch explanations)
- Exception Resolution Agent (auto-resolution capability matrix)

### Phase 3 — Health & Communication 
- SLA Monitor Agent (breach prediction, drift detection)
- Vendor Communication Agent (outbound email)
- Payment Authorization Agent (risk scoring, approval gates)
- `human_review_queue` + review UI in React

### Phase 4 — Dashboard & Tuning
- Agent health dashboard (throughput, error rates, automation rate)
- Confidence threshold tuning UI (adjustable per resolution type)
- Override feedback loop (human overrides → adjust thresholds)
- Audit report export (per-invoice PDF narrative)

---

## Key Files to Modify / Create

| File | Change |
|---|---|
| `agent_runtime.py` | **New** — base tool-calling loop |
| `agents/orchestrator.py` | **New** — workflow state machine agent |
| `agents/intake.py` | **New** — replaces `email_client.py` polling logic |
| `agents/extraction.py` | **New** — wraps `ocr_landingai.py` with validation |
| `agents/po_matching.py` | **New** — wraps + extends `po_matching.py` |
| `agents/exception_resolution.py` | **New** |
| `agents/sla_monitor.py` | **New** |
| `agents/payment_auth.py` | **New** — wraps `payments.py` |
| `agents/vendor_comms.py` | **New** |
| `audit_trail.py` | **New** — cross-cutting decision logger |
| `task_queue.py` | **New** — DB-backed task queue replacing APScheduler |
| `app.py` | Extend with agent management APIs |
| `invoice_db.py` | Add `workflow_states` read/write |
| `migrations/004_agent_tables.sql` | **New** — all new tables above |
| `frontend/src/pages/AgentDashboard.jsx` | **New** — agent health view |
| `frontend/src/pages/HumanReview.jsx` | **New** — review queue UI |
| `frontend/src/components/AuditTrail.jsx` | **New** — per-invoice audit view |

---

## What Makes This Genuinely Agentic

1. **Tool calling:** Every agent action is a Claude tool call — structured inputs, structured outputs, logged to audit trail.
2. **Planning loops:** Orchestrator runs an iterative loop — dispatch, await, evaluate, re-route.
3. **Durable task state:** Tasks persist in DB — survives process restarts, enables retry logic.
4. **Cross-agent delegation:** Exception Agent can call Vendor Comms Agent mid-resolution.
5. **Feedback loops:** Human overrides update confidence thresholds; SLA breaches trigger re-routing.
6. **Minimal human involvement:** Configurable confidence thresholds; humans see pre-structured decisions, not raw exceptions.
7. **Auditable reasoning:** Every decision has a `reasoning` field — not just what happened, but why.