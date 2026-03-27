CREATE TABLE IF NOT EXISTS public.payment_authorization_requests (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  review_item_id uuid REFERENCES public.human_review_queue(id) ON DELETE SET NULL,
  approval_status text NOT NULL DEFAULT 'pending_approval'
    CHECK (approval_status IN ('pending_approval', 'approved', 'rejected', 'payment_intent_created', 'execution_failed')),
  invoice_ids jsonb NOT NULL DEFAULT '[]'::jsonb,
  customer jsonb NOT NULL DEFAULT '{}'::jsonb,
  currency text,
  save_method boolean NOT NULL DEFAULT false,
  total_amount numeric,
  invoice_count integer NOT NULL DEFAULT 1,
  risk_level text NOT NULL DEFAULT 'medium'
    CHECK (risk_level IN ('low', 'medium', 'high')),
  recommendation text NOT NULL DEFAULT 'approval_required',
  risk_reasons jsonb NOT NULL DEFAULT '[]'::jsonb,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  approved_by text,
  approved_at timestamptz,
  rejected_by text,
  rejected_at timestamptz,
  executed_payment_id text,
  executed_payment_intent_id text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (invoice_count >= 1)
);

CREATE INDEX IF NOT EXISTS idx_payment_authorization_requests_status
  ON public.payment_authorization_requests(approval_status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_payment_authorization_requests_review_item
  ON public.payment_authorization_requests(review_item_id);
