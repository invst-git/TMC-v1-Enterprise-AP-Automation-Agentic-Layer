# Technical Walkthrough

## Overview

This application is an agentic accounts payable system. It receives vendor invoices, stores the original documents, extracts structured invoice data, validates the extraction, checks for duplicates, matches invoices against purchase orders, routes exceptions to human review, prepares vendor clarification drafts, authorizes payment batches, and executes approved payments through Stripe.

The system is built around a practical automation model rather than an unconstrained autonomous agent. Business-critical decisions are deterministic where possible, persisted in the database, exposed to operators, and gated by review queues when confidence or risk is not acceptable.

At a high level, the product has four major layers:

- A Flask backend that owns APIs, ingestion, orchestration, database operations, agent task execution, review actions, payment authorization, payment execution, chat, and live events.
- A PostgreSQL database that stores accounts payable records and the full agentic workflow backbone.
- A React frontend that acts as the operations console for dashboarding, vendors, exceptions, review, payments, and agent operations.
- External integrations for email intake, document extraction, LLM-assisted vendor chat, and Stripe payment processing.

## Core Domain Model

The core business objects are vendors, purchase orders, invoices, invoice lines, payments, chats, and workflow records.

Vendors represent supplier master data. They store identity, tax information, contact details, address information, recent invoice activity, and purchase order activity.

Purchase orders represent approved spend commitments. Each purchase order can have line items, totals, currency, delivery details, and a lifecycle status such as open or partially received.

Invoices represent extracted AP documents. An invoice stores supplier identity, invoice number, invoice dates, currency, totals, payment terms, PO reference, banking details, extracted file artifacts, vendor linkage, matching status, confidence, and line items.

Payments represent Stripe-backed payment intents and their relationship to one or more invoices. Payment records preserve the previous invoice status so failed or canceled payment attempts can safely revert invoices.

The agentic workflow model adds another set of durable records:

- Source documents track inbound files before they become invoices.
- Source document segments track invoice candidates split out of a larger PDF.
- Workflow states track the current lifecycle state of any important entity.
- Workflow history records state transitions and their reasons.
- Agent tasks model durable background work with retry, leasing, deduplication, and dead-letter behavior.
- Agent decisions record reasoning, confidence, model or ruleset identity, and metadata for auditability.
- Human review items model explicit human checkpoints.
- Vendor communications model approval-gated outbound clarification drafts.
- SLA configurations and breach risk fields model operational health.
- Payment authorization requests model approval state and risk analysis before Stripe execution.

## End-To-End Invoice Lifecycle

The invoice lifecycle starts when a document enters the system through either email intake or manual upload.

Email intake connects to an IMAP mailbox, looks back over recent messages, optionally filters senders, scans attachments, rejects attachments that do not look invoice-like, saves accepted files locally, registers them as source documents, and sends them into the processing pipeline.

Manual upload receives an operator-selected file and vendor, saves it using the same local storage strategy, registers the file as a source document, and then runs the same processing pipeline.

After a file is saved, the system creates a source document record. This is the first important design choice: the original inbound file is treated as a durable workflow entity, not just a temporary file that exists only while OCR is running. The source document stores metadata such as source type, original filename, storage path, content type, file size, file hash, sender metadata, vendor reference, ingestion status, segmentation status, and extraction status.

The system then records a workflow state for the source document and enqueues an intake classification task. Manual uploads are treated as explicit invoice candidates. Email attachments are classified through deterministic invoice attachment heuristics based on subject, filename, content type, and common invoice document types.

If the source file is a PDF, the segmentation layer analyzes page text and decides whether the PDF should remain one document or be split into multiple invoice candidates. It looks for page-level signals such as invoice headers, invoice numbers, page counters, totals near the bottom of a page, continuation wording, table-like text, due-date signals, and vendor anchors. When confidence is high enough, it writes segment PDFs and persists segment records. When confidence is not high enough, it keeps the file as one document rather than risking an unsafe split.

Each segment, or the original file when no split is used, is passed to the extraction layer. The extraction layer calls LandingAI ADE to parse the document, uses a Pydantic invoice schema to request structured fields, writes parse artifacts and extracted field JSON, and can pass targeted field hints during retry-style re-extraction.

The validation layer then checks the extracted data before persistence. It validates required fields, invoice total arithmetic, date sanity, duplicate candidates, and OCR output readability. The validation result is structured: it contains a decision, extraction status, issues, duplicate candidates, extracted fields, and optional review item linkage.

High-confidence duplicate invoices are blocked before insertion into the invoice table. This duplicate prevention happens during validation and again at persistence time, which gives the system defense in depth against double ingestion and double payment.

If validation does not block the document, the system persists the invoice and its line items. It then marks the invoice as received, links the relevant source segment to the invoice, publishes a live update, and starts purchase order matching.

## Matching And Exception Recovery

The first matching pass is deterministic. The matcher loads the invoice, validates that it has a PO reference and total amount, finds open or partially received purchase orders with the same PO number, and scores candidates based on vendor alignment, currency alignment, amount delta, and configured tolerances.

If a candidate is eligible, the invoice is automatically matched, linked to the purchase order, assigned a confidence score, and moved into an auto-matched state.

If deterministic matching cannot safely complete, the exception resolution layer runs before escalating to a human. This layer attempts several bounded recovery paths:

- Vendor fuzzy matching compares supplier names and tax IDs against known vendors and can relink the invoice when the best candidate is strong and unambiguous.
- Fuzzy PO matching compares PO references against open POs for the confirmed vendor and requires both similarity and amount compatibility.
- Tiered amount recovery can auto-link small variances, and can accept moderate variances only when there is vendor precedent.
- Targeted OCR re-extraction can retry low-confidence fields once, update the invoice if confidence improves, and then rerun matching.

Every recovery attempt is recorded as an agent decision with findings, outcome, confidence, and reasoning. If recovery succeeds, the invoice can still be automatically matched. If recovery does not succeed, the system creates a human review item with a review packet describing the original analysis, recovery attempts, final analysis, recommended action, candidate POs, and supporting metadata.

## Human Review

Human review is a first-class workflow, not an afterthought.

Review items can be created by extraction validation, PO matching, payment authorization, and vendor communication planning. Each item has an entity target, queue name, priority, status, reason, optional assignee, metadata, and resolution state.

Operators can assign review items, approve a candidate PO match, request vendor clarification, or reject a review item. These actions update the review item, update the relevant invoice or payment authorization state, write a human decision record, and publish a live event so the UI refreshes.

Approving a PO match moves the invoice into a manually matched or ready-for-payment path. Requesting vendor clarification creates a vendor communication draft and moves the invoice into a clarification-requested state. Rejecting a review item marks the invoice or related entity as rejected or dismissed.

## Vendor Communication

Vendor communication is intentionally approval-gated. The system does not silently send email to vendors.

When clarification is needed, the communication planner loads invoice and vendor context, selects a deterministic template based on the review reason, infers a recipient from invoice or vendor contact fields, creates a draft communication, records a planning task and decision, creates a workflow state, and inserts a human review item.

Supported draft scenarios include missing PO number, vendor mismatch, amount outside tolerance, and a generic clarification fallback. A human must approve or reject the draft. Once approved, a human can mark it as sent. The implementation tracks the communication lifecycle, but outbound email delivery itself is not automatically executed by the agent.

## Payment Authorization And Execution

Payments use a two-stage design: authorization first, Stripe execution second.

The payment authorization layer accepts a batch of invoice IDs and payer details. It normalizes invoice IDs, deduplicates the selection, loads invoices, verifies they are payable, enforces single-currency batches, calculates total amount, checks invoice count, checks vendor payment history, detects duplicate selections, and computes a low, medium, or high risk level.

Low-risk payment batches can be auto-approved and immediately converted into Stripe PaymentIntents. Medium- and high-risk batches are routed to the human review queue before any Stripe intent is created.

The authorization record stores invoice IDs, customer details, currency, save-method preference, total amount, invoice count, risk level, recommendation, risk reasons, review item linkage, approval state, approver or rejecter, and execution references.

Approved authorizations can be executed. Execution creates a completed payment task, calls the Stripe payment layer, records a payment execution decision, stores the Stripe PaymentIntent reference, and moves the authorization into a payment-intent-created state.

The Stripe payment layer locks invoices during payment intent creation, blocks already paid invoices, blocks invoices already pending under a different payment, blocks duplicate invoice signatures, enforces allowed statuses, and uses a stable idempotency key. If a payment succeeds, linked invoices become paid. If a payment fails or is canceled, invoices revert to their previous status.

## Agent Task Queue

The background task system is a durable queue stored in PostgreSQL. It supports task types such as intake classification, extraction validation, matching evaluation, exception resolution, payment authorization, payment routing, and payment execution.

Each task has a type, target entity, optional source document, priority, status, attempt count, max attempts, dedupe key, availability time, lease fields, heartbeat fields, payload, result, and error fields.

Workers claim queued tasks using a lease, mark them running, dispatch to a registered handler, and then complete, fail, requeue, or dead-letter them. Retry delay uses task-specific policies and exponential backoff where applicable. Payload validation failures are dead-lettered rather than retried.

The application can also run parts of the pipeline synchronously when the worker is disabled. This fallback matters for local development and for workflows where the operator expects immediate upload logs.

## SLA Monitoring And Operational Health

The SLA monitor keeps workflow states aligned with invoice statuses, seeds default SLA configurations when needed, computes how long invoices have been in their current states, and updates breach risk as ok, warning, breaching, or breached.

It also detects stalled agent tasks. If a stalled task still has retry budget, it is requeued. If it has exhausted attempts, it is dead-lettered. Both outcomes are recorded as decisions and can emit live updates.

The Agent Operations view uses this data to show automation rate, processing time, agent activity, exception outcomes, and SLA health over configurable windows.

## Realtime Update Model

The backend maintains an in-memory Server-Sent Events broadcaster. Whenever important actions happen, such as ingestion start, document segmentation, invoice persistence, matching updates, review queue changes, payment authorization changes, payment success, or SLA risk changes, the backend publishes a live event with a monotonically increasing revision.

The frontend subscribes to this stream through an EventSource connection. A shared live refresh provider tracks connection health, latest event time, and revision. Pages use a refresh hook that reloads data when revisions change, avoids overlapping refreshes, queues refreshes while one is already in flight, and skips refreshes when the browser tab is hidden.

This gives the operator a near-live operations console without requiring manual reloads.

## API Surface

The backend exposes endpoints for the major product surfaces:

- Dashboard metrics, graph data, recent invoices, invoice details, and invoice audit trails.
- Email intake run status and manual inbox checks.
- Manual invoice uploads.
- Vendor list, vendor stats, vendor details, vendor creation, vendor deletion, vendor-scoped mentions, and vendor chat.
- Purchase order details.
- Exception invoices.
- Payable invoices, payment history, pending confirmations, payment creation, payment confirmation, and payment cancellation.
- Agent overview, operations metrics, source documents, workflow states, workflow history, agent tasks, agent decisions, review queue items, review queue counts, SLA configs, SLA breaches, vendor communications, and payment authorizations.
- Mutation endpoints for assigning, resolving, and rejecting review items; drafting, approving, rejecting, and marking vendor communications sent; requesting, routing, approving, rejecting, and executing payment authorizations.

Errors are converted into operator-friendly responses. Duplicate invoices, duplicate payment attempts, mixed-currency selections, missing configuration, unavailable services, already-paid invoices, in-progress payment batches, missing review candidates, and vendor-scope mismatches receive specific messages and HTTP status codes.

## Frontend Walkthrough

The React application is organized around six main routes.

The dashboard is the operational landing page. It shows invoice volume, processed amount, productivity estimates, exception counts, graph data, recent invoices, inbox-check controls, and manual upload controls. It also opens invoice details and audit trails.

The vendors page shows vendor master data, vendor statistics, searchable vendor lists, vendor details, recent invoices, purchase orders, add/delete vendor actions, invoice detail modals, PO detail modals, and vendor-scoped chat.

The exceptions page groups invoices that need attention by vendor and status. It supports vendor and status filters and opens invoice detail views for inspection.

The review queue page is the human decision surface. It filters active review items, displays priority, review context, analysis summaries, recovery attempts, candidate PO data, and action forms for assignment, approval, vendor clarification, and rejection.

The payments page groups payable invoices by vendor, captures payer information, routes selected invoices through authorization, handles pending approval notices, resumes pending Stripe confirmations, confirms card payments, cancels failed attempts, and displays payment history.

The agent operations page summarizes automation and health: fully automated invoices, fast approvals, deeper human involvement, automation rate, average processing time, agent activity, exception outcomes, and SLA state.

Shared UI behavior includes page-level cache for faster navigation, live refresh on backend events, reusable modals for invoice and PO detail, a persistent sidebar with review counts, and API-level user-friendly error mapping.

## Vendor Chat

Vendor chat is scoped to a single vendor. The backend loads vendor context plus optional tagged invoices and purchase orders. The LLM prompt explicitly restricts answers to that vendor and the tagged records. If the configured LLM provider is unavailable, the system falls back to a deterministic response containing the available context.

Chats and messages are persisted. Messages can include tags so the frontend can attach invoice or PO references to a question. The backend supports both normal message generation and streamed responses.

## Database Safety And Consistency

The database layer uses PostgreSQL as the source of truth. Migrations define core AP tables, payment tables, chat tables, agent workflow tables, payment authorization tables, and SLA runtime fields.

Several safety controls are implemented close to persistence:

- Duplicate invoice candidates are checked before invoice insert.
- Duplicate payable invoices are filtered out of payable lists.
- Payment creation locks selected invoices before status changes.
- Payment records use Stripe intent uniqueness and local idempotency.
- Payment invoice links preserve previous invoice statuses for rollback.
- Agent task dedupe keys prevent duplicate active work.
- Workflow history preserves transitions rather than overwriting the narrative.
- Human review decisions are written as decision records for auditability.

## Configuration And Integrations

The backend reads configuration from environment variables. Important configuration includes database URL, Flask secret, IMAP settings, target senders, invoice storage directory, LandingAI API key and model, Stripe keys, LLM provider and API key, worker enablement, worker lease and polling settings, SLA monitor timing, validation tolerances, exception recovery thresholds, payment risk thresholds, and frontend Stripe publishable key.

LandingAI ADE is used for OCR parsing and schema-guided invoice field extraction.

Stripe is used for PaymentIntent creation, card confirmation, payment status retrieval, and payment state reconciliation.

Anthropic Claude is the primary LLM provider for vendor-scoped chat and title generation, with a deterministic fallback when unavailable. There is also a retained optional branch for Gemini-style generation.

IMAP is used for inbox polling and attachment retrieval.

Local filesystem storage is used for inbound invoices, segmented PDF outputs, parse artifacts, extracted field JSON, and extraction metadata.

## Test Coverage

The test suite covers the main risk-bearing behavior rather than only simple route smoke tests.

Coverage includes API route responses, source document registration, task queue deduplication, workflow state transitions, task claiming and retries, worker completion and dead-letter behavior, segmentation persistence, email ingress registration, extraction validation, duplicate blocking, deterministic PO matching, exception recovery paths, review queue actions, vendor communication approval gates, payment authorization routing, payment execution constraints, payment idempotency, duplicate payment blocking, SLA risk updates, stalled task recovery, source document finalization, and operator-friendly error mapping.

The tests reinforce the intended system boundaries: automation is allowed when confidence and policy permit it, but ambiguous extraction, matching, vendor communication, and payment decisions are made auditable and reviewable.

## Key Design Principles

The application favors durable workflow state over transient control flow. Important business steps are persisted so processing can be inspected, retried, audited, and surfaced in the UI.

Automation is bounded. Deterministic rules and structured confidence checks handle the common path, while review queues handle ambiguity and risk.

Duplicate prevention is layered. The system blocks duplicates during extraction validation, invoice persistence, payable invoice retrieval, and payment creation.

Human review is integrated into the state machine. Review actions update business records, workflow states, audit decisions, and live UI state.

Payments are approval-gated and idempotent. The system separates risk evaluation from Stripe execution and carefully prevents double-payment failure modes.

The UI is operational rather than static. Server-Sent Events drive refreshes across dashboard, vendors, exceptions, review, payments, invoice detail, and agent operations views.

Auditability is central. Agent decisions, human decisions, workflow history, review metadata, payment authorization records, and invoice audit trails provide a clear record of what happened and why.

## Current Implementation Boundaries

The system is agentic in structure, but much of the current intelligence is deterministic and rules-based. That is an intentional safety posture for accounts payable workflows.

Vendor communication is draft-and-track only. A human approval and send step is required; the system does not automatically email vendors.

The OCR layer depends on LandingAI configuration. If the API key is unavailable, extraction returns no JSON and the pipeline routes accordingly.

The worker can be disabled. In that mode, upload and intake paths use synchronous processing where possible.

Local file storage is used for documents and artifacts. The database stores paths and metadata, not the binary documents themselves.

The frontend relies on the backend API and live event stream. If the event stream is unavailable, pages can still fetch data, but live connection state will be marked stale.

## Summary

This codebase implements an end-to-end AP automation system with durable document tracking, invoice extraction, validation, duplicate protection, purchase order matching, exception recovery, human review, approval-gated vendor communication, payment authorization, Stripe execution, vendor-scoped chat, SLA monitoring, audit trails, and realtime operations visibility.

The most important architectural idea is that every significant business event is represented as data: a source document, segment, invoice, task, decision, workflow state, review item, communication, authorization request, or payment record. That makes the system inspectable, recoverable, and safe enough for workflows where incorrect automation can create real financial risk.
