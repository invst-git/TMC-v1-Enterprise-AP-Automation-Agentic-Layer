CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE OR REPLACE FUNCTION public.set_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

CREATE TABLE IF NOT EXISTS public.vendors (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name text NOT NULL,
  tax_id text,
  contact_info text,
  address text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.purchase_orders (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  po_number text NOT NULL,
  vendor_id uuid REFERENCES public.vendors(id) ON DELETE SET NULL,
  buyer_company_name text,
  bill_to_address text,
  ship_to_address text,
  shipping_method text,
  shipping_terms text,
  delivery_date_expected date,
  payment_terms text,
  currency text NOT NULL DEFAULT 'USD',
  subtotal_amount numeric(14,2),
  tax_amount numeric(14,2),
  shipping_amount numeric(14,2),
  total_amount numeric(14,2),
  status text NOT NULL DEFAULT 'open',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.purchase_order_lines (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  po_id uuid NOT NULL REFERENCES public.purchase_orders(id) ON DELETE CASCADE,
  line_number integer,
  sku text,
  description text,
  quantity_ordered numeric(14,4),
  unit_of_measure text,
  unit_price numeric(14,4),
  line_total numeric(14,2),
  tax_rate numeric(10,4),
  tax_code text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.invoices (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  supplier_name text,
  supplier_email text,
  supplier_tax_id text,
  supplier_address text,
  buyer_name text,
  company_code text,
  cost_center text,
  invoice_number text,
  invoice_date date,
  due_date date,
  currency text NOT NULL DEFAULT 'USD',
  payment_terms text,
  subtotal_amount numeric(14,2),
  tax_amount numeric(14,2),
  shipping_amount numeric(14,2),
  discount_amount numeric(14,2),
  total_amount numeric(14,2),
  po_number text,
  bank_account text,
  swift_bic text,
  remittance_reference text,
  invoice_type text,
  file_path text,
  fields_json_path text,
  email_message_id text,
  vendor_id uuid REFERENCES public.vendors(id) ON DELETE SET NULL,
  matched_po_id uuid REFERENCES public.purchase_orders(id) ON DELETE SET NULL,
  status text NOT NULL DEFAULT 'unmatched',
  confidence numeric(10,4),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.invoice_lines (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  invoice_id uuid NOT NULL REFERENCES public.invoices(id) ON DELETE CASCADE,
  line_number integer,
  description text,
  sku text,
  quantity numeric(14,4),
  unit_of_measure text,
  unit_price numeric(14,4),
  line_total numeric(14,2),
  tax_rate numeric(10,4),
  tax_code text,
  po_number text,
  po_line_number text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_vendors_tax_id_unique
  ON public.vendors(tax_id)
  WHERE tax_id IS NOT NULL AND btrim(tax_id) <> '';

CREATE INDEX IF NOT EXISTS idx_vendors_name_lower
  ON public.vendors((lower(name)));

CREATE INDEX IF NOT EXISTS idx_purchase_orders_vendor_status
  ON public.purchase_orders(vendor_id, status);

CREATE INDEX IF NOT EXISTS idx_purchase_orders_po_number
  ON public.purchase_orders(po_number);

CREATE INDEX IF NOT EXISTS idx_purchase_orders_created_at
  ON public.purchase_orders(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_purchase_order_lines_po_id_line_number
  ON public.purchase_order_lines(po_id, line_number);

CREATE INDEX IF NOT EXISTS idx_invoices_vendor_status
  ON public.invoices(vendor_id, status);

CREATE INDEX IF NOT EXISTS idx_invoices_created_at
  ON public.invoices(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_invoices_invoice_date
  ON public.invoices(invoice_date DESC);

CREATE INDEX IF NOT EXISTS idx_invoices_invoice_number_lower
  ON public.invoices((lower(invoice_number)));

CREATE INDEX IF NOT EXISTS idx_invoices_po_number
  ON public.invoices(po_number);

CREATE INDEX IF NOT EXISTS idx_invoices_email_message_id
  ON public.invoices(email_message_id);

CREATE INDEX IF NOT EXISTS idx_invoices_matched_po_id
  ON public.invoices(matched_po_id);

CREATE INDEX IF NOT EXISTS idx_invoices_supplier_name_lower
  ON public.invoices((lower(supplier_name)));

CREATE INDEX IF NOT EXISTS idx_invoice_lines_invoice_id_line_number
  ON public.invoice_lines(invoice_id, line_number);

DROP TRIGGER IF EXISTS trg_vendors_set_updated_at ON public.vendors;
CREATE TRIGGER trg_vendors_set_updated_at
BEFORE UPDATE ON public.vendors
FOR EACH ROW
EXECUTE FUNCTION public.set_updated_at();

DROP TRIGGER IF EXISTS trg_purchase_orders_set_updated_at ON public.purchase_orders;
CREATE TRIGGER trg_purchase_orders_set_updated_at
BEFORE UPDATE ON public.purchase_orders
FOR EACH ROW
EXECUTE FUNCTION public.set_updated_at();

DROP TRIGGER IF EXISTS trg_purchase_order_lines_set_updated_at ON public.purchase_order_lines;
CREATE TRIGGER trg_purchase_order_lines_set_updated_at
BEFORE UPDATE ON public.purchase_order_lines
FOR EACH ROW
EXECUTE FUNCTION public.set_updated_at();

DROP TRIGGER IF EXISTS trg_invoices_set_updated_at ON public.invoices;
CREATE TRIGGER trg_invoices_set_updated_at
BEFORE UPDATE ON public.invoices
FOR EACH ROW
EXECUTE FUNCTION public.set_updated_at();

DROP TRIGGER IF EXISTS trg_invoice_lines_set_updated_at ON public.invoice_lines;
CREATE TRIGGER trg_invoice_lines_set_updated_at
BEFORE UPDATE ON public.invoice_lines
FOR EACH ROW
EXECUTE FUNCTION public.set_updated_at();
