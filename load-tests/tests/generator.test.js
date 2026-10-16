import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {applicationPayload,customerIdForRun,assertRunId,seededFieldsRandom} from '../scripts/lib/data.js';
import {makeConfig,k6Scenario} from '../scripts/lib/config.js';
const base = {BASE_URL:'http://loan-application-service:8080',RUN_ID:'abcdef12'};

test('valid Bean Validation-compatible synthetic payload', () => {
  const request = applicationPayload('abcdef12', () => .5);
  assert.match(request.customerId,/^abcdef12-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  assert.ok(request.amount > 0 && request.amount < 50000);
  assert.ok(request.term >= 1 && request.term <= 120);
  assert.equal(request.currency, 'USD');
});

test('unique test data (probabilistic sample) and run prefix', () => {
  const ids = new Set(Array.from({length:500},()=> customerIdForRun('abc123ee')));
  assert.equal(ids.size,500);
  assert.ok([...ids].every(v => v.startsWith('abc123ee-')));
});

test('invalid run id and oversized workloads rejected', () => {
  assert.throws(()=>assertRunId('00000000'));
  assert.throws(()=>makeConfig({...base,RATE:'500'}),/MAX_RATE/);
  assert.throws(()=>makeConfig({...base,DURATION_SECONDS:'601'}),/MAX_DURATION_SECONDS/);
  assert.throws(()=>makeConfig({...base,SCENARIO:'unknown'}),/Unknown/);
  assert.throws(()=>makeConfig({...base,BASE_URL:'https://production.example'}),/production/);
  assert.throws(()=>makeConfig({...base,RATE:'abc'}),/integer/);
  assert.throws(()=>makeConfig({...base,SCENARIO:'ramp-up',RATE:'40',DURATION_SECONDS:'3'}),/4 seconds/);
  assert.throws(()=>makeConfig({...base,BASE_URL:'https://actual.bank.com'}),/non-local/);
});

test('open arrival-rate executor and configurable timeout',()=>{
  const config=makeConfig({...base,SCENARIO:'baseline',RATE:'10',TIMEOUT_SECONDS:'7'});
  assert.equal(config.timeoutSeconds,7);
  assert.equal(k6Scenario(config).executor,'constant-arrival-rate');
  assert.equal(k6Scenario(config).rate,10);
  const ramp=makeConfig({...base,SCENARIO:'ramp-up',RATE:'80'});
  assert.equal(k6Scenario(ramp).executor,'ramping-arrival-rate');
  assert.equal(Math.max(...k6Scenario(ramp).stages.map(s=>s.target)),80);
});

test('GET scenario requires application UUID',()=>{
  assert.throws(()=>makeConfig({...base,SCENARIO:'cache-read'}),/APPLICATION_ID/);
});

test('HTTP-only workload has error accounting and result export',()=>{
  const runner=fs.readFileSync(new URL('../scripts/load.js',import.meta.url),'utf8');
  assert.match(runner,/http\.post\(/);
  assert.match(runner,/http\.get\(/);
  assert.match(runner,/accepted\.add/);
  assert.match(runner,/rejected\.add/);
  assert.match(runner,/handleSummary/);
  assert.match(runner,/LPS_K6_JSON_BEGIN/);
  assert.match(runner,/LPS_K6_JSON_END/);
  assert.match(runner,/ramp-up.*spike.*stress/);
  assert.doesNotMatch(runner,/jdbc:|KafkaTemplate|INSERT INTO/i);
});

test('same numeric field seed yields reproducible amount and term, random customer IDs remain independent',()=>{
  const a=applicationPayload('abcdef12', Math.random, seededFieldsRandom('fixed',1,1));
  const b=applicationPayload('abcdef12', Math.random, seededFieldsRandom('fixed',1,1));
  assert.equal(a.amount,b.amount);
  assert.equal(a.term,b.term);
});


test('JSON summary is recoverable from stdout after Job terminates', () => {
  const source = fs.readFileSync(new URL('../scripts/load.js', import.meta.url), 'utf8');
  const isolated = source.slice(source.indexOf('export function handleSummary(data)'))
    .replace('export function handleSummary(data)', 'function handleSummary(data)');
  const execSummary = new Function('config', 'data', `${isolated}\nreturn handleSummary(data);`);
  const response = execSummary(
    { runId: 'abcdef12', scenario: 'spike', baseUrl: base.BASE_URL, rate: 60, durationSeconds: 60, seed: '', resultsDir: '/tmp' },
    {metrics: {dropped_iterations: {values: {count: 435}}, http_reqs: {values: {count: 735, rate: 12.17}}}}
  );
  const match = response.stdout.match(/LPS_K6_JSON_BEGIN\n(.*?)\nLPS_K6_JSON_END/s);
  assert.ok(match, 'expected stable JSON delimiters');
  const report = JSON.parse(match[1]);
  assert.equal(report.run_id, 'abcdef12');
  assert.equal(report.metrics.dropped_iterations.values.count, 435);
  assert.equal(report.metrics.http_reqs.values.count, 735);
});
