CREATE TABLE IF NOT EXISTS public.source_documents (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  source_type text NOT NULL CHECK (source_type IN ('email_attachment', 'manual_upload', 'api_upload', 'system')),
  source_ref text,
  original_filename text,
  storage_provider text NOT NULL DEFAULT 'local',
  storage_path text NOT NULL UNIQUE,
  content_type text,
  file_size_bytes bigint,
  file_hash text,
  page_count integer,
  from_email text,
  email_message_id text,
  vendor_id uuid,
  ingestion_status text NOT NULL DEFAULT 'received',
  segmentation_status text NOT NULL DEFAULT 'not_started',
  extraction_status text NOT NULL DEFAULT 'not_started',
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  received_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (file_size_bytes IS NULL OR file_size_bytes >= 0),
  CHECK (page_count IS NULL OR page_count >= 1)
);

CREATE TABLE IF NOT EXISTS public.source_document_segments (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  source_document_id uuid NOT NULL REFERENCES public.source_documents(id) ON DELETE CASCADE,
  segment_index integer NOT NULL,
  page_from integer NOT NULL,
  page_to integer NOT NULL,
  segment_path text,
  confidence numeric,
  status text NOT NULL DEFAULT 'created',
  invoice_id uuid REFERENCES public.invoices(id) ON DELETE SET NULL,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (source_document_id, segment_index),
  UNIQUE (source_document_id, page_from, page_to),
  CHECK (segment_index >= 1),
  CHECK (page_from >= 1),
  CHECK (page_to >= page_from)
);

CREATE TABLE IF NOT EXISTS public.workflow_states (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_type text NOT NULL,
  entity_id uuid NOT NULL,
  current_state text NOT NULL,
  current_stage text,
  confidence numeric,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (entity_type, entity_id)
);

CREATE TABLE IF NOT EXISTS public.workflow_state_history (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_type text NOT NULL,
  entity_id uuid NOT NULL,
  from_state text,
  to_state text NOT NULL,
  event_type text NOT NULL,
  reason text,
  actor_type text NOT NULL DEFAULT 'system',
  actor_id text,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.agent_tasks (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  task_type text NOT NULL,
  entity_type text NOT NULL,
  entity_id uuid NOT NULL,
  source_document_id uuid REFERENCES public.source_documents(id) ON DELETE SET NULL,
  priority integer NOT NULL DEFAULT 100,
  status text NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'leased', 'running', 'completed', 'failed', 'canceled', 'dead_letter')),
  attempt_count integer NOT NULL DEFAULT 0,
  max_attempts integer NOT NULL DEFAULT 5,
  dedupe_key text,
  available_at timestamptz NOT NULL DEFAULT now(),
  lease_expires_at timestamptz,
  locked_by text,
  locked_at timestamptz,
  heartbeat_at timestamptz,
  started_at timestamptz,
  completed_at timestamptz,
  last_error text,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  result jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (priority >= 0),
  CHECK (attempt_count >= 0),
  CHECK (max_attempts >= 1)
);

CREATE TABLE IF NOT EXISTS public.agent_decisions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  task_id uuid NOT NULL REFERENCES public.agent_tasks(id) ON DELETE CASCADE,
  entity_type text NOT NULL,
  entity_id uuid NOT NULL,
  agent_name text NOT NULL,
  model_name text,
  prompt_version text,
  decision_type text NOT NULL,
  decision text NOT NULL,
  confidence numeric,
  reasoning_summary text,
  tool_calls jsonb NOT NULL DEFAULT '[]'::jsonb,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.human_review_queue (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_type text NOT NULL,
  entity_id uuid NOT NULL,
  source_document_id uuid REFERENCES public.source_documents(id) ON DELETE SET NULL,
  invoice_id uuid REFERENCES public.invoices(id) ON DELETE SET NULL,
  queue_name text NOT NULL DEFAULT 'default',
  priority integer NOT NULL DEFAULT 100,
  status text NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'assigned', 'resolved', 'dismissed')),
  review_reason text NOT NULL,
  assigned_to text,
  due_at timestamptz,
  resolution text,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  resolved_at timestamptz,
  CHECK (priority >= 0)
);

CREATE TABLE IF NOT EXISTS public.vendor_communications (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  vendor_id uuid,
  invoice_id uuid REFERENCES public.invoices(id) ON DELETE SET NULL,
  source_document_id uuid REFERENCES public.source_documents(id) ON DELETE SET NULL,
  direction text NOT NULL CHECK (direction IN ('draft', 'outbound', 'inbound')),
  channel text NOT NULL DEFAULT 'email',
  status text NOT NULL DEFAULT 'draft',
  recipient text,
  subject text,
  body text,
  approved_by text,
  sent_at timestamptz,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.sla_configs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_type text NOT NULL,
  state_name text NOT NULL,
  target_minutes integer NOT NULL,
  escalation_queue text,
  is_active boolean NOT NULL DEFAULT true,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (entity_type, state_name),
  CHECK (target_minutes > 0)
);

CREATE INDEX IF NOT EXISTS idx_source_documents_ingestion_status
  ON public.source_documents(ingestion_status, received_at DESC);
CREATE INDEX IF NOT EXISTS idx_source_documents_email_message_id
  ON public.source_documents(email_message_id);
CREATE INDEX IF NOT EXISTS idx_source_documents_vendor_id
  ON public.source_documents(vendor_id);

CREATE INDEX IF NOT EXISTS idx_source_document_segments_source_document_id
  ON public.source_document_segments(source_document_id, segment_index);
CREATE INDEX IF NOT EXISTS idx_source_document_segments_invoice_id
  ON public.source_document_segments(invoice_id);

CREATE INDEX IF NOT EXISTS idx_workflow_states_state
  ON public.workflow_states(entity_type, current_state);
CREATE INDEX IF NOT EXISTS idx_workflow_state_history_entity
  ON public.workflow_state_history(entity_type, entity_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_agent_tasks_queue
  ON public.agent_tasks(status, available_at, priority, created_at);
CREATE INDEX IF NOT EXISTS idx_agent_tasks_entity
  ON public.agent_tasks(entity_type, entity_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_agent_tasks_source_document_id
  ON public.agent_tasks(source_document_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_tasks_active_dedupe_key
  ON public.agent_tasks(dedupe_key)
  WHERE dedupe_key IS NOT NULL AND status IN ('queued', 'leased', 'running');

CREATE INDEX IF NOT EXISTS idx_agent_decisions_task_id
  ON public.agent_decisions(task_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_agent_decisions_entity
  ON public.agent_decisions(entity_type, entity_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_human_review_queue_open
  ON public.human_review_queue(status, priority, created_at);
CREATE INDEX IF NOT EXISTS idx_human_review_queue_entity
  ON public.human_review_queue(entity_type, entity_id);

CREATE INDEX IF NOT EXISTS idx_vendor_communications_vendor_id
  ON public.vendor_communications(vendor_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_vendor_communications_invoice_id
  ON public.vendor_communications(invoice_id);

CREATE INDEX IF NOT EXISTS idx_sla_configs_active
  ON public.sla_configs(entity_type, state_name)
  WHERE is_active = true;
