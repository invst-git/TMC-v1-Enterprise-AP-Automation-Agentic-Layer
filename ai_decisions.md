# Key AI Decisions

## Overview

This application uses AI as part of an accounts payable workflow, but it does not treat AI as an unrestricted decision-maker. The central architectural decision is to combine AI-shaped workflow decomposition with deterministic controls, durable state, confidence scoring, audit trails, and human approval gates.

The result is an agentic system that can automate a large portion of invoice processing while preserving the safety properties expected in a financial workflow.

The key idea is simple:

- AI and automation should accelerate intake, extraction, matching, exception investigation, vendor communication planning, and payment routing.
- Financially sensitive actions should remain explainable, reversible where possible, and gated when confidence or risk is not acceptable.
- Every meaningful automated decision should leave behind structured evidence.

This document describes the decisions that matter most for judging the architecture.

## Decision 1: Build A Constrained Agentic System

The system is intentionally agentic, but not autonomous in the loose sense.

Instead of giving a general-purpose LLM control over the whole procure-to-pay lifecycle, the application decomposes the lifecycle into bounded agents and workflow stages. Each stage has a clear responsibility, a narrow set of allowed actions, and a durable record of what it decided.

The important design choice is that the system behaves like an agentic workflow engine, not a chatbot bolted onto invoice processing.

The application has agents or agent-like modules for:

- Intake classification.
- Extraction validation.
- Purchase order matching.
- Exception recovery.
- Human review orchestration.
- Vendor communication planning.
- Payment authorization.
- SLA and task health monitoring.

Each of these areas produces structured outcomes. They do not simply return free-form text. They write workflow state, task results, decisions, confidence values, reasons, and review metadata.

This matters because accounts payable is not a domain where vague AI output is acceptable. The system must answer: what happened, why did it happen, who or what decided it, what evidence was used, and what action is allowed next.

## Decision 2: Use Deterministic Rules For High-Risk Core Decisions

The system deliberately keeps several critical decisions deterministic.

Invoice duplicate detection, PO matching eligibility, payment eligibility, payment risk routing, review item state transitions, task retry behavior, and payment status updates are rule-driven rather than delegated to an LLM.

This is a safety decision.

LLMs are useful for understanding unstructured data and supporting human interaction, but they are not the right primitive for every financial control. If an invoice should not be paid twice, that rule should be enforced through database checks and deterministic comparisons. If a payment batch mixes currencies, it should be rejected predictably. If a payment requires approval, that approval state should be explicit.

The system uses AI-adjacent reasoning where ambiguity exists, but anchors the final state transitions in deterministic code and database records.

The benefit is that the application can still be judged as agentic without relying on opaque autonomy for the riskiest behavior.

## Decision 3: Treat Source Documents As First-Class Workflow Entities

A major architectural decision is that the original inbound document is tracked before it becomes an invoice.

Many invoice-processing systems treat the uploaded file as a temporary input and only persist the invoice after OCR succeeds. This application does not do that. It records a source document as soon as the file is received.

That enables the system to track:

- Where the document came from.
- Who sent it.
- Which file was stored.
- Whether it was classified.
- Whether it was segmented.
- Whether extraction started, failed, completed, or required review.
- Which invoice or invoices eventually came from it.

This is important for agentic architecture because the first entity in the workflow is not the invoice. The first entity is the document. A single document may produce no invoices, one invoice, multiple invoice segments, a duplicate block, or a review item.

By modeling this explicitly, the system can reason about the lifecycle before structured invoice data exists.

## Decision 4: Split Multi-Invoice PDFs Conservatively

The PDF segmentation logic is designed to be conservative.

The system looks for page-level invoice boundary signals, but only splits a PDF when confidence is high enough. If the system is unsure, it keeps the PDF together.

This is the right failure mode for AP automation. A false split can corrupt evidence, extract partial invoices, duplicate records, or create incorrect payment candidates. A missed split is less dangerous because the document can still be routed to review or manually handled.

The AI decision here is not simply “detect invoice boundaries.” It is “detect invoice boundaries only when the evidence is strong enough to justify changing the processing unit.”

That distinction matters. The system optimizes for safe automation, not maximum automation at any cost.

## Decision 5: Validate Extraction Before Persistence

The extraction layer does not blindly persist whatever the OCR provider returns.

After structured fields are extracted, the system validates them. It checks required fields, total arithmetic, date sanity, readability, and duplicate candidates. The validation result can decide to persist, route to review, fail, or block persistence entirely.

This is one of the most important AI safety decisions in the application.

OCR and document extraction are probabilistic. Even a strong model can misread an invoice number, supplier name, total amount, due date, or PO number. Persisting bad extraction as if it were fact would contaminate downstream matching and payment logic.

The system therefore inserts a validation stage between AI extraction and business persistence.

This makes AI output advisory until it passes business checks.

## Decision 6: Block Duplicates Early And Repeatedly

Duplicate prevention is not implemented as a single late-stage check. It appears at multiple points in the workflow.

The system checks for duplicate invoice candidates during extraction validation, blocks strong duplicates before persistence, checks again during invoice insert, filters duplicates out of payable invoice lists, and blocks duplicate invoice signatures during payment creation.

This layered approach reflects the risk profile of accounts payable.

Duplicate invoices are one of the highest-value failure modes to prevent. If the system only checked duplicates at payment time, operators could still spend time reviewing and matching duplicate records. If the system only checked duplicates at ingestion time, later payment paths could still be exposed to records created by other channels or earlier versions of the system.

The decision is to enforce duplicate protection as a cross-cutting invariant rather than as a single feature.

## Decision 7: Use Exception Recovery Before Human Escalation

The system does not send every failed match directly to a human.

When deterministic PO matching fails, the exception recovery layer tries bounded recovery paths first. It can attempt vendor fuzzy matching, fuzzy PO matching, amount tolerance recovery, and targeted OCR re-extraction.

This is a strong agentic design choice. The system is not just classifying failures; it investigates them.

However, each recovery path is constrained:

- Vendor relinking requires strong and unambiguous confidence.
- Fuzzy PO matching requires similarity, currency compatibility, and amount compatibility.
- Moderate amount variance requires vendor precedent.
- OCR re-extraction is targeted and limited.
- Unresolved cases still go to human review with a complete review packet.

The result is practical autonomy. The system can recover common exceptions without human work, but it does not invent resolutions when evidence is weak.

## Decision 8: Record Recovery Attempts As Decisions

Every exception recovery attempt is treated as an auditable decision.

This is more important than it may look. If the system only stored the final result, operators and judges would not know whether an invoice was easy to match, recovered by fuzzy logic, recovered after OCR retry, or escalated after multiple failed attempts.

The application records the path name, outcome, summary, findings, confidence, and reasoning for recovery attempts.

This creates a narrative:

- What failed initially.
- What the system tried.
- Which evidence supported or rejected each attempt.
- Whether the final state was automated or escalated.

That audit trail is what makes the architecture defensible. Automation is not invisible.

## Decision 9: Make Human Review A Product Primitive

Human review is not a fallback log message. It is a real domain object.

The review queue has status, priority, reason, assignee, resolution, metadata, candidate records, and timestamps. Review actions update the underlying business entity and also write decision records.

This decision is central to safe AI adoption.

A weaker system might simply mark invoices as failed and expect an operator to inspect raw data. This application creates review packets with the reason, analysis, recovery attempts, candidate POs, recommended action, and supporting metadata.

The goal is not just to ask a human to decide. The goal is to give the human the best available context so the review is fast, consistent, and auditable.

## Decision 10: Gate Vendor Communication

The system can draft vendor clarification emails, but it does not automatically send them.

This is an intentional boundary. Vendor communication has business, legal, and relationship impact. A badly worded or incorrect message can create confusion, reveal internal process details, or damage trust.

The application therefore treats vendor communication as a planned action:

- The system drafts the message.
- The draft is stored.
- The reason and recipient inference are recorded.
- A review item is created.
- A human approves or rejects it.
- Sending is tracked as a human-marked event.

The AI value is in reducing the effort to prepare a clear clarification. The control remains with the operator.

## Decision 11: Separate Payment Authorization From Payment Execution

Payment is the highest-risk stage in the workflow. The system separates the question “should this batch be allowed to proceed?” from the action “create a Stripe payment intent.”

Payment authorization analyzes the batch before execution. It checks invoice eligibility, currency consistency, invoice count, total amount, vendor payment history, and duplicate selections. It assigns a risk level and recommendation.

Low-risk batches can be auto-approved. Medium- and high-risk batches go to review before any payment intent is created.

This architecture avoids a common mistake: combining payment button behavior with payment risk evaluation. In this system, risk routing is its own workflow with its own record, review item, approval state, decision log, and execution state.

That separation makes payment automation safer and easier to audit.

## Decision 12: Use Idempotency And Status Locks In Payment Flow

The payment flow is designed for retry safety.

Stripe payment intent creation uses a stable idempotency key. Local payment records are reused for the same Stripe intent. Invoices are locked during payment creation. Invoices already paid or already pending under another payment are rejected. If payment fails or is canceled, invoice statuses are reverted using the previous status stored in the payment link.

This is not an AI-specific decision, but it is critical to an AI-enabled payment architecture.

Once automation can initiate or route payment batches, the payment layer must be resilient to retries, duplicate clicks, worker delays, and stale UI state. The system therefore treats payment safety as a database and workflow concern, not a frontend concern.

## Decision 13: Keep Live UI State Synchronized

The system uses live backend events to keep the operations UI current.

This matters because agentic workflows are asynchronous. Intake, extraction, matching, recovery, review, authorization, and payment confirmation can happen at different times. If the UI only showed stale snapshots, operators could act on outdated states.

The live event model publishes changes when key workflow events occur. The frontend listens to these events and refreshes pages when the backend revision changes.

The architectural decision is that realtime visibility is part of the safety model. Operators should not need to guess whether an invoice was just matched, blocked, moved to review, authorized, or paid.

## Decision 14: Use LLMs For Contextual Assistance, Not Core State Authority

The clearest LLM use case in the application is vendor-scoped chat. The assistant can answer questions about a selected vendor and optionally tagged invoices or purchase orders.

The LLM is constrained by context. It is instructed to answer only from the selected vendor and tagged records. If the LLM provider is unavailable, the system falls back to deterministic context output.

This is a deliberate placement of LLM capability. The LLM helps operators understand context, but it does not own invoice persistence, PO matching, review resolution, payment authorization, or Stripe execution.

That separation gives the product useful AI interaction without allowing conversational output to mutate financial records directly.

## Decision 15: Make Confidence Operational

Confidence is not used as decoration. It affects routing and interpretation.

The system attaches confidence to extraction validation, matching outcomes, recovery attempts, workflow states, vendor communication planning, payment risk recommendations, and SLA decisions.

Confidence helps decide:

- Whether a document can continue.
- Whether an exception can be recovered automatically.
- Whether a vendor match is strong enough.
- Whether a PO candidate is safe enough.
- Whether a communication draft has a reliable recipient.
- Whether a payment batch can be auto-approved.

The important choice is that confidence is paired with reasons and evidence. A confidence number alone is not enough. The system stores confidence with the decision type, reasoning summary, metadata, and affected entity.

## Decision 16: Prefer Review Packets Over Raw Errors

When automation cannot proceed, the system tries to produce a review packet rather than only an error.

A review packet can include extracted fields, duplicate candidates, initial matching analysis, final matching analysis, recovery attempts, candidate purchase orders, risk signals, recommended actions, and operator-facing summaries.

This improves both product usability and architectural credibility. A failed AI step should not leave the human with less information than before. The system should explain what it knows, what it tried, where it became uncertain, and what decision is needed.

## Decision 17: Use Workflow History As The Audit Backbone

The system records current state, but it also records state history.

Current state is useful for screens and filters. History is useful for audit, debugging, and trust.

For an invoice, a reviewer or judge can reconstruct the sequence from received document to extraction, persistence, matching, exception recovery, review, payment readiness, authorization, and payment. For payment authorization, the system can show risk routing, approval, execution, and Stripe intent creation. For vendor communication, it can show draft, approval, rejection, and sent status.

This makes the architecture suitable for financial operations because the system is not only optimized for throughput. It is optimized for explainability after the fact.

## Decision 18: Allow Sync And Async Processing

The application supports both synchronous processing and asynchronous worker processing.

When the worker is enabled, extraction, matching, recovery, and payment routing can run through durable agent tasks. When the worker is disabled, the application can still process synchronously for local development and simpler operational modes.

This flexibility is useful, but the more important decision is that both modes are aligned around the same workflow concepts. Synchronous processing still records source documents, validation results, matching decisions, review items, workflow states, and live events where possible.

That keeps the architecture coherent even when deployment mode changes.

## Decision 19: Surface AI Operations As A Product Area

The application includes an Agent Operations view rather than hiding automation health inside logs.

This reflects a mature AI product decision. If the system is judged on agentic behavior, it needs to show how the agents are performing:

- Automation rate.
- Fully automated invoices.
- Fast approvals.
- Deeper human involvement.
- Average processing time.
- Agent activity.
- Exception outcomes.
- SLA health.

This lets operators and reviewers evaluate whether automation is actually reducing work, where humans are still needed, and where the workflow is at risk.

## Decision 20: Treat Missing Configuration As An Operator-Facing State

The application maps missing services, missing migrations, unavailable databases, OCR failures, payment conflicts, and review-state conflicts into user-facing errors.

This matters because AI systems often fail at the boundaries: missing API keys, external service outages, stale database schema, partial setup, or delayed background work.

Instead of exposing raw stack traces to the frontend, the system converts common failure modes into messages an operator can understand and act on.

That improves trust. A system that fails clearly is safer than one that fails mysteriously.

## Why This Architecture Is Agentic

The application is agentic because the workflow is decomposed into decision-making units that act on durable entities, produce structured outputs, and move work forward without a human at every step.

It is not merely an OCR pipeline because it does more than extract text.

It classifies intake, decides whether to segment documents, validates extracted fields, blocks duplicates, matches invoices, investigates exceptions, retries targeted extraction, produces review packets, plans vendor communication, evaluates payment risk, routes payment approvals, monitors SLAs, recovers stalled tasks, and keeps the UI synchronized.

Most importantly, these actions are not invisible. They are represented as tasks, decisions, states, history events, review items, and authorization records.

That is the architectural difference between automation and agentic operations.

## Why This Architecture Is Safe

The safety model is based on constraints rather than trust in a model.

The system is safe because:

- AI extraction is validated before persistence.
- Duplicate prevention exists at multiple layers.
- PO matching requires explicit eligibility checks.
- Exception recovery is bounded by confidence and evidence.
- Ambiguous cases go to human review.
- Vendor communication requires approval.
- Medium- and high-risk payments require approval.
- Stripe execution is separated from payment authorization.
- Payment creation is idempotent and locked.
- All meaningful decisions are recorded.
- The UI receives live updates when state changes.

This is the right posture for an AP system. The system automates where evidence is strong and escalates where judgment is needed.

## Why This Architecture Is Judgable

For a jury, the strength of the architecture is not only that it uses AI. The strength is that it shows disciplined AI product design.

The system demonstrates:

- Clear workflow decomposition.
- Durable state for every important lifecycle stage.
- Bounded autonomy.
- Confidence-aware routing.
- Human-in-the-loop control.
- Audit-first decision records.
- Recovery before escalation.
- Payment safety controls.
- Operational visibility.
- Graceful degradation when services are missing.

The design can be evaluated from the outside because decisions are observable. A judge can inspect what the system did, why it did it, when it escalated, and how it prevented unsafe automation.

## Summary

The most important AI decision in the application is restraint.

The system does not use AI to bypass accounting controls. It uses AI and agentic workflow design to reduce manual effort while strengthening traceability.

The architecture is built around a practical principle: automate the common path, investigate recoverable exceptions, escalate ambiguous decisions, and never let high-risk financial actions happen without explicit evidence and policy gates.

That is what makes the application both agentic and suitable for real accounts payable operations.
