ALTER TABLE public.workflow_states
  ADD COLUMN IF NOT EXISTS breach_risk text NOT NULL DEFAULT 'ok';

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'workflow_states_breach_risk_check'
  ) THEN
    ALTER TABLE public.workflow_states
      ADD CONSTRAINT workflow_states_breach_risk_check
      CHECK (breach_risk IN ('ok', 'warning', 'breaching', 'breached'));
  END IF;
END $$;

ALTER TABLE public.sla_configs
  ADD COLUMN IF NOT EXISTS warning_minutes integer,
  ADD COLUMN IF NOT EXISTS breach_minutes integer;

UPDATE public.sla_configs
SET
  breach_minutes = COALESCE(breach_minutes, target_minutes),
  warning_minutes = COALESCE(warning_minutes, GREATEST(1, FLOOR(COALESCE(target_minutes, 1) / 2.0)::integer))
WHERE breach_minutes IS NULL OR warning_minutes IS NULL;

ALTER TABLE public.sla_configs
  ALTER COLUMN warning_minutes SET NOT NULL,
  ALTER COLUMN breach_minutes SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'sla_configs_warning_minutes_check'
  ) THEN
    ALTER TABLE public.sla_configs
      ADD CONSTRAINT sla_configs_warning_minutes_check
      CHECK (warning_minutes > 0);
  END IF;
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'sla_configs_breach_minutes_check'
  ) THEN
    ALTER TABLE public.sla_configs
      ADD CONSTRAINT sla_configs_breach_minutes_check
      CHECK (breach_minutes >= warning_minutes);
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_workflow_states_breach_risk
  ON public.workflow_states(entity_type, breach_risk, current_state);
