BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

INSERT INTO public.vendors (
  name,
  tax_id,
  contact_info,
  address
)
SELECT
  'Northwind Industrial Supply',
  'NW-AP-001',
  'ap@northwind.example',
  '101 Foundry Road, Chicago, IL'
WHERE NOT EXISTS (
  SELECT 1
  FROM public.vendors
  WHERE tax_id = 'NW-AP-001'
);

INSERT INTO public.vendors (
  name,
  tax_id,
  contact_info,
  address
)
SELECT
  'Atlas Office Goods',
  'AT-AP-002',
  'billing@atlas.example',
  '22 Market Square, Austin, TX'
WHERE NOT EXISTS (
  SELECT 1
  FROM public.vendors
  WHERE tax_id = 'AT-AP-002'
);

INSERT INTO public.purchase_orders (
  po_number,
  vendor_id,
  buyer_company_name,
  bill_to_address,
  ship_to_address,
  shipping_method,
  shipping_terms,
  delivery_date_expected,
  payment_terms,
  currency,
  subtotal_amount,
  tax_amount,
  shipping_amount,
  total_amount,
  status
)
SELECT
  'PO-DEMO-2001',
  v.id,
  'The Matching Company',
  '1 Finance Way, New York, NY',
  '500 Warehouse Lane, Newark, NJ',
  'Ground',
  'FOB Destination',
  CURRENT_DATE + INTERVAL '7 days',
  'Net 30',
  'USD',
  1750.00,
  175.00,
  25.00,
  1950.00,
  'open'
FROM public.vendors v
WHERE v.tax_id = 'NW-AP-001'
  AND NOT EXISTS (
    SELECT 1
    FROM public.purchase_orders p
    WHERE p.po_number = 'PO-DEMO-2001'
  );

INSERT INTO public.purchase_orders (
  po_number,
  vendor_id,
  buyer_company_name,
  bill_to_address,
  ship_to_address,
  shipping_method,
  shipping_terms,
  delivery_date_expected,
  payment_terms,
  currency,
  subtotal_amount,
  tax_amount,
  shipping_amount,
  total_amount,
  status
)
SELECT
  'PO-DEMO-2002',
  v.id,
  'The Matching Company',
  '1 Finance Way, New York, NY',
  '75 Operations Drive, Dallas, TX',
  'Air',
  'Prepaid',
  CURRENT_DATE + INTERVAL '10 days',
  'Net 15',
  'USD',
  820.00,
  82.00,
  18.00,
  920.00,
  'open'
FROM public.vendors v
WHERE v.tax_id = 'AT-AP-002'
  AND NOT EXISTS (
    SELECT 1
    FROM public.purchase_orders p
    WHERE p.po_number = 'PO-DEMO-2002'
  );

INSERT INTO public.purchase_order_lines (
  po_id,
  line_number,
  sku,
  description,
  quantity_ordered,
  unit_of_measure,
  unit_price,
  line_total,
  tax_rate,
  tax_code
)
SELECT
  p.id,
  1,
  'NW-BRG-100',
  'Industrial bearings',
  10,
  'EA',
  100.0000,
  1000.00,
  10.0000,
  'STD'
FROM public.purchase_orders p
WHERE p.po_number = 'PO-DEMO-2001'
  AND NOT EXISTS (
    SELECT 1
    FROM public.purchase_order_lines pol
    WHERE pol.po_id = p.id
      AND pol.line_number = 1
  );

INSERT INTO public.purchase_order_lines (
  po_id,
  line_number,
  sku,
  description,
  quantity_ordered,
  unit_of_measure,
  unit_price,
  line_total,
  tax_rate,
  tax_code
)
SELECT
  p.id,
  2,
  'NW-LUB-250',
  'Machine lubricant',
  15,
  'EA',
  50.0000,
  750.00,
  10.0000,
  'STD'
FROM public.purchase_orders p
WHERE p.po_number = 'PO-DEMO-2001'
  AND NOT EXISTS (
    SELECT 1
    FROM public.purchase_order_lines pol
    WHERE pol.po_id = p.id
      AND pol.line_number = 2
  );

INSERT INTO public.purchase_order_lines (
  po_id,
  line_number,
  sku,
  description,
  quantity_ordered,
  unit_of_measure,
  unit_price,
  line_total,
  tax_rate,
  tax_code
)
SELECT
  p.id,
  1,
  'AT-CHR-020',
  'Office chairs',
  4,
  'EA',
  120.0000,
  480.00,
  10.0000,
  'STD'
FROM public.purchase_orders p
WHERE p.po_number = 'PO-DEMO-2002'
  AND NOT EXISTS (
    SELECT 1
    FROM public.purchase_order_lines pol
    WHERE pol.po_id = p.id
      AND pol.line_number = 1
  );

INSERT INTO public.purchase_order_lines (
  po_id,
  line_number,
  sku,
  description,
  quantity_ordered,
  unit_of_measure,
  unit_price,
  line_total,
  tax_rate,
  tax_code
)
SELECT
  p.id,
  2,
  'AT-DSK-010',
  'Standing desks',
  2,
  'EA',
  170.0000,
  340.00,
  10.0000,
  'STD'
FROM public.purchase_orders p
WHERE p.po_number = 'PO-DEMO-2002'
  AND NOT EXISTS (
    SELECT 1
    FROM public.purchase_order_lines pol
    WHERE pol.po_id = p.id
      AND pol.line_number = 2
  );

COMMIT;
