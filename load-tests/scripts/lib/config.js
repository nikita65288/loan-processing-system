// Strict safety settings: a typo must not accidentally create an unbounded test.
const SCENARIOS = ['smoke', 'baseline', 'ramp-up', 'target', 'steady-state', 'spike', 'stress', 'soak', 'cache-read'];
export function intEnv(env, name, fallback, lower = 1) {
  const raw = env[name] === undefined || env[name] === '' ? String(fallback) : env[name];
  if (!/^[0-9]+$/.test(String(raw))) throw new Error(`${name} must be a nonnegative integer`);
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < lower) throw new Error(`Invalid ${name}: ${raw}`);
  return value;
}
export function makeConfig(env) {
  const scenario = env.SCENARIO || 'smoke';
  if (!SCENARIOS.includes(scenario)) throw new Error(`Unknown SCENARIO: ${scenario}`);
  const baseUrl = (env.BASE_URL || '').replace(/\/$/, '');
  if (!/^https?:\/\/[^/]+(?:\/.*)?$/.test(baseUrl)) throw new Error('BASE_URL must be an HTTP(S) URL');
  if (/(^|[.\/])(prod|production)([.\/:]|$)/i.test(baseUrl)) throw new Error('Refusing production-looking BASE_URL');
  const host = baseUrl.match(/^https?:\/\/([^/:?#]+)/i)[1].toLowerCase();
  const allowed = ['127.0.0.1','localhost','host.docker.internal','loan-application-service','loan-application-service.lps.svc.cluster.local'];
  if (!allowed.includes(host)) throw new Error(`Refusing non-local BASE_URL host: ${host}`);
  const runId = env.RUN_ID || '';
  if (!/^[a-f0-9]{8}$/i.test(runId) || /^0{8}$/.test(runId)) throw new Error('RUN_ID must be 8 nonzero hex characters');
  const defaults = { smoke:2, baseline:10, 'ramp-up':100, target:500, 'steady-state':20, spike:60, stress:150, soak:10, 'cache-read':20 };
  const durations = {smoke:20,baseline:60,'ramp-up':120,target:60,'steady-state':180,spike:60,stress:120,soak:600,'cache-read':60};
  const resultsDir = env.RESULTS_DIR || '/results';
  if (!['/results','/tmp'].includes(resultsDir)) throw new Error('Invalid RESULTS_DIR');
  const maxRate = intEnv(env,'MAX_RATE',100);
  const maxSeconds = intEnv(env,'MAX_DURATION_SECONDS',600);
  const rate = intEnv(env,'RATE',defaults[scenario]);
  const durationSeconds = intEnv(env,'DURATION_SECONDS',durations[scenario]);
  if (['ramp-up','spike','stress'].includes(scenario) && durationSeconds < 4) throw new Error('Ramp scenarios need at least 4 seconds');
  const preVUs = intEnv(env,'VUS',Math.max(10,Math.ceil(rate / 5)));
  const maxVUs = intEnv(env,'MAX_VUS',Math.max(preVUs,Math.ceil(rate / 2)));
  const timeoutSeconds = intEnv(env,'TIMEOUT_SECONDS',5);
  if (rate > maxRate) throw new Error(`RATE ${rate} > MAX_RATE ${maxRate}`);
  if (durationSeconds > maxSeconds) throw new Error(`DURATION_SECONDS ${durationSeconds} > MAX_DURATION_SECONDS ${maxSeconds}`);
  if (preVUs > maxVUs) throw new Error('VUS exceeds MAX_VUS');
  if (maxVUs > 1000) throw new Error('MAX_VUS may not exceed 1000');
  if (scenario === 'cache-read' && !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(env.APPLICATION_ID || '')) {
    throw new Error('APPLICATION_ID UUID required for cache-read');
  }
  return { scenario, baseUrl, runId:runId.toLowerCase(), rate, durationSeconds, preVUs, maxVUs,
    timeoutSeconds, applicationId: env.APPLICATION_ID || '', seed: env.SEED || '', resultsDir };
}

export function k6Scenario(config) {
  const base = { timeUnit:'1s', preAllocatedVUs:config.preVUs, maxVUs:config.maxVUs };
  if (!['ramp-up','spike','stress'].includes(config.scenario)) {
    return { executor:'constant-arrival-rate', rate:config.rate, duration:`${config.durationSeconds}s`, ...base };
  }
  // Linear arrival-rate ramps. Targets never exceed the validated RATE safety ceiling.
  const stage = Math.floor(config.durationSeconds / 4);
  const tail = config.durationSeconds - stage * 3;
  const low = Math.max(1,Math.floor(config.rate / 10));
  const mid = Math.max(low,Math.floor(config.rate / 2));
  let rates;
  if (config.scenario === 'spike') rates = [low, low, config.rate, low];
  else if (config.scenario === 'stress') rates = [low, mid, config.rate, config.rate];
  else rates = [low, mid, config.rate, config.rate];
  return { executor:'ramping-arrival-rate', startRate:low, stages:rates.map((target,i) => ({duration:`${i===3 ? tail : stage}s`,target})), ...base };
}
