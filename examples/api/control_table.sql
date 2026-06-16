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
  method        STRING  COMMENT 'HTTP method; the shared fetch job issues GET (see generator note)',
  params        STRING  COMMENT 'optional query string, e.g. postId=1 -> fetched as /comments?postId=1',
  schedule      STRING  COMMENT 'quartz cron for the domain job (keep it identical for every row in a domain)',
  enabled       BOOLEAN COMMENT 'false = skip this endpoint without deleting the row'
)
COMMENT 'YAIF API module: one row per endpoint, tagged with the domain it belongs to.';

-- Same rows as control_table.csv. (jsonplaceholder paths, so this also runs against
-- the yaif_demo_api demo connection.)
INSERT INTO main.config.api_endpoints VALUES
  ('blog',     'posts',    '/posts',    'GET', NULL,       '0 0 */4 * * ?', true),
  ('blog',     'comments', '/comments', 'GET', 'postId=1', '0 0 */4 * * ?', true),
  ('gallery',  'albums',   '/albums',   'GET', NULL,       '0 30 2 * * ?',  true),
  ('gallery',  'photos',   '/photos',   'GET', NULL,       '0 30 2 * * ?',  true),
  ('accounts', 'users',    '/users',    'GET', NULL,       '0 0 6 * * ?',   true),
  ('accounts', 'todos',    '/todos',    'GET', NULL,       '0 0 6 * * ?',   false);
