import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

function read(name) { return fs.readFileSync(path.join(root, name), 'utf8'); }

test('structured reconciliation SQL is read-only and scoped to RUN_ID', () => {
  const sql = read('sql/run-summary.sql');
  assert.match(sql, /left\(customer_id::text, 8\)\s*=\s*'__RUN_ID__'/);
  assert.match(sql, /duplicate_disbursement_application_ids/);
  assert.match(sql, /saga_requires_reconciliation/);
  assert.match(sql, /outbox_statuses/);
  assert.match(sql, /global_inbox_messages_not_scoped_to_run/);
  // Strip SQL comments before inspecting executable SQL.
  const executable = sql.replace(/--[^\n]*/g, '');
  assert.doesNotMatch(executable, /\b(INSERT|UPDATE|DELETE|TRUNCATE|ALTER|DROP|CREATE|GRANT|REVOKE)\b/i);
});

test('runner forwards RUN_ID, and always attempts report collection on k6 failure', () => {
  const wrapper = read('run-and-report.ps1');
  assert.match(wrapper, /\$runnerArgs\s*=\s*@\{\s*Scenario=\$Scenario;\s*RunId=\$RunId\s*\}/);
  assert.match(wrapper, /catch\s*\{[\s\S]*?runnerError/);
  assert.match(wrapper, /'collect-report.ps1'/);
});

test('report executes all nine original SQL checks and saves first/final snapshots', () => {
  const collector = read('collect-report.ps1');
  for (const phrase of ['post-run.sql','post-run-all-nine-queries.txt','snapshot-initial.json',
    'snapshot-final.json','report.md','report-data.json']) {
    assert.ok(collector.includes(phrase), `missing ${phrase}`);
  }
  assert.match(collector, /docker compose exec -T postgres/);
  assert.match(collector, /\$DrainTimeoutSeconds/);
});

test('kubectl Job runner includes prior JSON-export hotfix markers', () => {
  const script = read('run-k8s.ps1');
  const load = read('scripts/load.js');
  assert.match(script, /LPS_K6_JSON_BEGIN/);
  assert.match(load, /LPS_K6_JSON_BEGIN/);
});
