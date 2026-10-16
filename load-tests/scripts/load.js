import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate } from 'k6/metrics';
import { makeConfig, k6Scenario } from './lib/config.js';
import { applicationPayload, seededFieldsRandom } from './lib/data.js';

// Exactly one API request per arrival-rate iteration; no hidden database or Kafka writes.
const config = makeConfig(__ENV);
const accepted = new Counter('lps_accepted_posts');
const rejected = new Counter('lps_failed_posts');
const businessOk = new Rate('lps_http_business_ok');

// Baseline-style runs must hit the offered rate; exploratory ramp/spike/stress
// runs record dropped iterations without turning an overload observation into a Job failure.
const thresholds = {
  'http_req_failed': [{threshold:'rate<0.20', abortOnFail:true, delayAbortEval:'15s'}],
  'lps_http_business_ok': ['rate>0.80']
};
if (!['ramp-up', 'spike', 'stress'].includes(config.scenario)) {
  thresholds['dropped_iterations'] = ['count<1'];
}

export const options = {
  scenarios: { [config.scenario]: k6Scenario(config) },
  thresholds,
  // Retain error bodies to diagnose validation failures (not credentials or PII).
  discardResponseBodies: false,
  summaryTrendStats: ['avg','med','p(95)','p(99)','max']
};

export default function () {
  const isRead = config.scenario === 'cache-read';
  const res = isRead
    ? http.get(`${config.baseUrl}/applications/${config.applicationId}`, {timeout:`${config.timeoutSeconds}s`,tags:{operation:'get_application'}})
    : http.post(`${config.baseUrl}/applications`, JSON.stringify(applicationPayload(config.runId, Math.random, config.seed ? seededFieldsRandom(config.seed, __VU, __ITER) : Math.random)), {
        headers:{'Content-Type':'application/json'},timeout:`${config.timeoutSeconds}s`,tags:{operation:'create_application'}
      });
  let validId = false;
  if (res.status === (isRead ? 200 : 201)) {
    try {
      const body = res.json();
      validId = typeof body.id === 'string' && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(body.id);
      if (isRead) validId = validId && body.id.toLowerCase() === config.applicationId.toLowerCase();
    } catch (_) { /* Invalid JSON is a failed business check, even after HTTP 201. */ }
  }
  const ok = res.status === (isRead ? 200 : 201) && validId;
  check(res, { 'expected status and application id': () => ok });
  businessOk.add(ok);
  if (!isRead) { if (ok) accepted.add(1); else rejected.add(1); }
}

// Short summary file; response IDs are correlated via synthetic customer_id prefix in SQL.
export function handleSummary(data) {
  const result = {
    run_id:config.runId, scenario:config.scenario, base_url:config.baseUrl,
    target_rate:config.rate, duration_seconds:config.durationSeconds, seed:config.seed || null,
    timestamp_utc:new Date().toISOString(),
    metrics:data.metrics,
    warning:'k6 captures HTTP performance only. Verify durable Saga outcomes with sql/post-run.sql.'
  };
  const metrics = data.metrics || {};
  const value = (name, field) => metrics[name]?.values?.[field] ?? 'n/a';
  const lines = [
    `Run ${config.runId} | scenario ${config.scenario} | target ${config.rate}/s`,
    `Requests: ${value('http_reqs','count')} | achieved requests/s: ${value('http_reqs','rate')}`,
    `Accepted POST: ${value('lps_accepted_posts','count')} | rejected POST: ${value('lps_failed_posts','count')}`,
    `HTTP duration (ms) p50=${value('http_req_duration','med')} p95=${value('http_req_duration','p(95)')} p99=${value('http_req_duration','p(99)')}`,
    `Dropped iterations: ${value('dropped_iterations','count')} | HTTP failed rate: ${value('http_req_failed','rate')}`,
    `Summary: ${config.resultsDir}/${config.runId}-${config.scenario}-summary.json`,
    'IMPORTANT: count durable Saga terminal states and backlog after the run using SQL + Prometheus.'
  ];
  // kubectl cp/exec cannot reliably retrieve a file once the k6 container exits.
  // Emit JSON between delimiters so PowerShell can extract it from immutable Pod logs,
  // including when k6 exits nonzero because a threshold was crossed.
  const serialized = JSON.stringify(result);
  return {
    [`${config.resultsDir}/${config.runId}-${config.scenario}-summary.json`]:JSON.stringify(result,null,2),
    stdout:lines.join('\n')+'\nLPS_K6_JSON_BEGIN\n'+serialized+'\nLPS_K6_JSON_END\n'
  };
}
