-- YAIF API module — endpoint control table (Unity Catalog version)
--
-- This is the governed, runtime source of truth for "which endpoints belong to
-- which business domain". It is kept in sync, column-for-column, with
-- examples/api/control_table.csv:
--
--   CSV  = quick start / local — hand it to the generator with `--csv` and you
--          can produce domain YAML without touching a workspace.
--   SQL  = the Unity Catalog table the pipelines read at runtime — create it once,
--          then point the generator at it with `--table <catalog>.config.api_endpoints`.
--
-- Adding, pausing, or removing an endpoint is then an INSERT / UPDATE here — no
-- code change. Re-run scripts/generate_api_domains.py to refresh the domain YAML.
--
-- Replace `main` below with your catalog (the bundle's var.catalog).

CREATE SCHEMA IF NOT EXISTS main.config;

CREATE TABLE IF NOT EXISTS main.config.api_endpoints (
  domain        STRING  COMMENT 'business domain -> one deployable YAIF unit (schema + pipeline + job)',
  endpoint_name STRING  COMMENT 'human-friendly label for the endpoint (not read at runtime)',
  path          STRING  COMMENT 'request path appended to the connection base_path, e.g. /orders',
  method        STRING  COMMENT 'HTTP method; GET (default) or POST. POST rows must carry a body.',
  params        STRING  COMMENT 'optional query string, e.g. postId=1 -> fetched as /comments?postId=1',
  schedule      STRING  COMMENT 'quartz cron for the domain job (keep it identical for every row in a domain)',
  enabled       BOOLEAN COMMENT 'false = skip this endpoint without deleting the row',
  -- Optional trailing columns (NULL/empty = safe default). The generator emits the
  -- legacy `api_endpoints` CSV when every row is GET-with-no-body; otherwise it emits
  -- `api_endpoints_json`. auth_mode/silver_shape are per-domain (the generator takes
  -- the first non-default value in a domain and warns on a conflict).
  body          STRING  COMMENT 'optional JSON request body for POST endpoints (sent as-is)',
  auth_mode     STRING  COMMENT 'per-domain auth: connection (default, UC HTTP connection) | basic_secret (direct requests + secret scope)',
  silver_shape  STRING  COMMENT 'per-domain silver: records (default, array-of-records) | document (one VARIANT row per response)'
)
COMMENT 'YAIF API module: one row per endpoint, tagged with the domain it belongs to.';

-- Same rows as control_table.csv. (jsonplaceholder paths, so this also runs against
-- the yaif_demo_api demo connection.) All GET-with-no-body, so the generator emits the
-- legacy api_endpoints CSV and the output is byte-for-byte the historical blog.yml.
INSERT INTO main.config.api_endpoints VALUES
  ('blog',     'posts',    '/posts',    'GET', NULL,       '0 0 */4 * * ?', true,  NULL, NULL, NULL),
  ('blog',     'comments', '/comments', 'GET', 'postId=1', '0 0 */4 * * ?', true,  NULL, NULL, NULL),
  ('gallery',  'albums',   '/albums',   'GET', NULL,       '0 30 2 * * ?',  true,  NULL, NULL, NULL),
  ('gallery',  'photos',   '/photos',   'GET', NULL,       '0 30 2 * * ?',  true,  NULL, NULL, NULL),
  ('accounts', 'users',    '/users',    'GET', NULL,       '0 0 6 * * ?',   true,  NULL, NULL, NULL),
  ('accounts', 'todos',    '/todos',    'GET', NULL,       '0 0 6 * * ?',   false, NULL, NULL, NULL);

-- A POST-with-body, basic_secret, document-shape domain looks like this (the generator
-- would emit api_endpoints_json + auth/shape plumbing). Host/creds are NEVER stored here
-- — auth_mode=basic_secret reads them from a UC secret scope at runtime, and the
-- generated YAML uses ${var.*} placeholders for the base URL/scope.
--   ('epm', 'exportdataslice', '/.../exportdataslice', 'POST', NULL, '0 0 6 * * ?', true,
--    '{"gridDefinition": {"pov": {...}, "rows": [...], "columns": [...]}}', 'basic_secret', 'document');
