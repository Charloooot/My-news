/**
 * WRLD_NEWS edge proxy — Cloudflare Worker.
 *
 * Why this exists:
 *   The dashboard is 100% client-side, so without a proxy every visitor hits
 *   GDELT / the article reader directly and gets rate-limited (GDELT allows only
 *   ~1 request / 5s per IP). This Worker fetches once, caches at the edge, and
 *   shares the result across all visitors — per-visitor throttling disappears.
 *
 * Endpoints (all return permissive CORS):
 *   GET /news                             → merged multi-source feed (see
 *                                           news-collector.js), rebuilt every
 *                                           15 min by the Cron Trigger
 *   GET /fr                               → French politics desk (10 national
 *                                           outlets), same cron
 *   GET /us                               → US politics desk (10 national outlets)
 *   GET /polls                            → voting intentions and election results
 *                                           for the POLITICS tab (polls-collector.js)
 *   GET /biz                              → business / tech / entertainment desk
 *                                           (15 outlets) for the IMPACT tab
 *   GET /conflicts                        → conflict intelligence for the CONFLICTS
 *                                           tab: daily coverage history, GDELT
 *                                           30-day timelines, ACLED events (opt.),
 *                                           UNHCR displacement — own cron, 30 min
 *   GET /quotes?symbols=DIS,MC.PA&range=5d → compact price series (Yahoo),
 *                                           edge-cached 5 min per symbol
 *   GET /caps?ids=Q312,Q2283              → dated market caps from Wikidata (7 d)
 *   GET /cams/caltrans?d=1..12            → compact Caltrans camera index (1 h)
 *   GET /cams/nz | /cams/es | /cams/is | /cams/bc → NZTA, Spain DGT, Iceland, DriveBC indexes (1 h)
 *   GET /cams/nps                         → US National Park webcams (24 h)
 *   GET /gdelt?query=...&maxrecords=250   → GDELT ArtList JSON, edge-cached 10 min
 *   GET /extract?url=https://...          → readable article text, edge-cached 6 h
 *   GET /tle?group=active                 → CelesTrak GP TLE, edge-cached 2 h
 *   GET /satcat                           → CelesTrak SATCAT csv, edge-cached 24 h
 *
 * Why /tle and /satcat exist:
 *   CelesTrak rate-limits per IP: it returns HTTP 403 ("GP data has not updated
 *   since your last successful download") if the same GROUP is re-fetched inside
 *   its 2-hour refresh window. A browser reloading the page — or many visitors
 *   behind one IP — trips this constantly. The Worker fetches once, edge-caches,
 *   and serves everyone from cache, so the 403 never reaches the client.
 *
 * Deploy (free tier is plenty) — full steps in wrangler.toml:
 *   1. npx wrangler login
 *   2. npx wrangler kv namespace create NEWS   → paste the id into wrangler.toml
 *   3. npx wrangler deploy
 *   4. Put the resulting https://<name>.<you>.workers.dev origin into
 *      index.html → const API_PROXY = 'https://<name>.<you>.workers.dev';
 *   Optional sources: npx wrangler secret put GUARDIAN_KEY  (and ACLED_USER /
 *   ACLED_PASS). Without them those sources are simply skipped.
 *
 * Storage: the merged feed lives in KV (binding NEWS) when configured, else in
 * the edge cache. KV is better — the cron writes it once and every colo reads
 * the same copy; the edge cache is per-colo, so each region rebuilds lazily.
 */

import { collectNews, collectFrance, collectUsPolitics, collectBusiness, fetchQuote, searchCompanies } from './news-collector.js';
import { collectPolls } from './polls-collector.js';
import { collectConflicts } from './conflicts-collector.js';
import { caltransDistrict, nzCameras, spainCameras, icelandCameras, npsWebcams, bcCameras } from './cams.js';

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
};

export default {
  async fetch(request, env, ctx) {
    if (request.method === 'OPTIONS') return new Response(null, { headers: CORS });

    const url = new URL(request.url);
    try {
      if (url.pathname === '/news')    return await handleFeed(FEEDS.world, env, ctx);
      if (url.pathname === '/fr')      return await handleFeed(FEEDS.fr, env, ctx);
      if (url.pathname === '/us')      return await handleFeed(FEEDS.us, env, ctx);
      if (url.pathname === '/polls')   return await handleFeed(FEEDS.polls, env, ctx);
      if (url.pathname === '/biz')     return await handleFeed(FEEDS.biz, env, ctx);
      if (url.pathname === '/conflicts') return await handleFeed(FEEDS.conflicts, env, ctx);
      if (url.pathname === '/quotes')  return await handleQuotes(url, ctx);
      if (url.pathname === '/search')  return await handleSearch(url, ctx);
      if (url.pathname.startsWith('/cams/')) return await handleCams(url, env, ctx);
      if (url.pathname === '/caps')    return await handleCaps(url, ctx);
      if (url.pathname === '/gdelt')   return await handleGdelt(url);
      if (url.pathname === '/extract') return await handleExtract(url);
      if (url.pathname === '/tle')     return await handleTle(url, env, ctx);
      if (url.pathname === '/satcat')  return await handleSatcat(url);
      if (url.pathname === '/' || url.pathname === '/health')
        return json({ ok: true, service: 'wrld-news proxy' });
    } catch (err) {
      return json({ error: String(err && err.message || err) }, 502);
    }
    return json({ error: 'not found' }, 404);
  },

  // Cron Trigger (wrangler.toml → [triggers] crons) — rebuild the merged feed.
  // Each cron runs its own invocation, so each gets the full subrequest
  // budget (50 on the free plan) — running all feeds in one invocation
  // overflowed it and silently dropped the last GDELT slice.
  async scheduled(event, env, ctx) {
    if (event.cron === TLE_CRON) {
      ctx.waitUntil(refreshTle(env).catch(err => console.warn('tle refresh:', err.message)));
      return;
    }
    const names = CRON_PLAN[event.cron] || Object.keys(FEEDS);   // unknown cron (e.g. local test) → everything
    ctx.waitUntil(Promise.allSettled(names.map(n => refreshFeed(FEEDS[n], env))));
  },
};

// ── /news and /fr ───────────────────────────────────────────────────────────
// Both feeds are built by the cron, stored in KV (or the edge cache when no KV
// is bound) and served as-is. A missing/very old copy is rebuilt on demand.
const FEEDS = {
  world: { key: 'news:latest', collect: collectNews },
  fr:    { key: 'fr:latest',   collect: collectFrance },
  us:    { key: 'us:latest',   collect: collectUsPolitics },
  biz:   { key: 'biz:latest',  collect: collectBusiness },
  // Polls move slowly, but election-week seat counts do not: same 15-min cron,
  // rebuilt on request only past a day (Wikipedia is asked as rarely as possible).
  polls: { key: 'polls:latest', collect: collectPolls, freshMs: 60 * 60e3, maxStaleMs: 24 * 3600e3 },
  // Slow-moving and GDELT-throttled: refreshed by its own 30-min cron, never
  // rebuilt in the request path unless it is very old.
  conflicts: { key: 'conflicts:latest', collect: collectConflicts, freshMs: 90 * 60e3, maxStaleMs: 24 * 3600e3 },
};
// Must match the crons in wrangler.toml.
const CRON_PLAN = {
  '*/15 * * * *':       ['world'],
  '7,22,37,52 * * * *': ['fr', 'biz'],
  '11,41 * * * *':      ['conflicts'],
  '3,18,33,48 * * * *': ['us', 'polls'],
};
const FEED_FRESH_MS = 20 * 60 * 1000;    // cron runs every 15 min
const FEED_MAX_STALE_MS = 6 * 3600e3;    // past this, rebuild before answering

async function readStoredFeed(feed, env) {
  if (env.NEWS) {
    const txt = await env.NEWS.get(feed.key);
    return txt ? { txt, generated: peekGenerated(txt) } : null;
  }
  const hit = await caches.default.match(new Request('https://cache/' + feed.key));
  if (!hit) return null;
  const txt = await hit.text();
  return { txt, generated: peekGenerated(txt) };
}
// `generated` is the first field, so read it without parsing ~1 MB of JSON.
function peekGenerated(txt) {
  const m = /"generated":"([^"]+)"/.exec(txt.slice(0, 120));
  return m ? Date.parse(m[1]) : 0;
}

async function refreshFeed(feed, env) {
  let previous = null;
  try { const st = await readStoredFeed(feed, env); if (st) previous = JSON.parse(st.txt); } catch {}
  const data = await feed.collect(env, previous);
  // Never replace a good feed with an empty one (all sources down at once).
  if (!data.count) return null;
  const txt = JSON.stringify(data);
  if (env.NEWS) {
    await env.NEWS.put(feed.key, txt);
  } else {
    await caches.default.put(new Request('https://cache/' + feed.key), new Response(txt, {
      headers: { 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=86400' },
    }));
  }
  return txt;
}

async function handleFeed(feed, env, ctx) {
  const stored = await readStoredFeed(feed, env);
  const age = stored ? Date.now() - stored.generated : Infinity;
  let txt = stored?.txt;
  if (!txt || age > (feed.maxStaleMs || FEED_MAX_STALE_MS)) {
    txt = (await refreshFeed(feed, env)) || txt;        // cold start / cron missing
  } else if (age > (feed.freshMs || FEED_FRESH_MS)) {
    ctx.waitUntil(refreshFeed(feed, env).catch(() => {}));  // serve stale, rebuild behind
  }
  if (!txt) return json({ error: 'all sources unavailable', articles: [] }, 503);
  return new Response(txt, {
    headers: { 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=120', ...CORS },
  });
}

// ── /cams ──────────────────────────────────────────────────────────────────
const CAM_INDEXES = {
  nz:  { ttl: 3600,  load: () => nzCameras() },
  es:  { ttl: 3600,  load: () => spainCameras() },
  is:  { ttl: 3600,  load: () => icelandCameras() },
  bc:  { ttl: 3600,  load: () => bcCameras() },
  nps: { ttl: 86400, load: (env) => npsWebcams(env.NPS_KEY || 'DEMO_KEY') },   // DEMO_KEY is rate-limited → cache a day
};
async function handleCams(reqUrl, env, ctx) {
  const name = reqUrl.pathname.slice('/cams/'.length);
  let load, ttl, keyStr;
  if (name === 'caltrans') {
    const d = Number(reqUrl.searchParams.get('d'));
    if (!(Number.isInteger(d) && d >= 1 && d <= 12)) return json({ error: 'd must be 1..12' }, 400);
    load = () => caltransDistrict(d); ttl = 3600; keyStr = `caltrans/${d}`;
  } else if (CAM_INDEXES[name]) {
    ({ load, ttl } = CAM_INDEXES[name]); keyStr = name;
  } else return json({ error: 'unknown camera index' }, 404);
  const key = new Request('https://cache/cams/' + keyStr);
  const hit = await caches.default.match(key);
  if (hit) return withCors(hit);
  const rows = await load(env);
  const resp = new Response(JSON.stringify(rows), {
    headers: { 'Content-Type': 'application/json', 'Cache-Control': `public, max-age=${ttl}`, ...CORS },
  });
  ctx.waitUntil(caches.default.put(key, resp.clone()));
  return resp;
}

// ── /caps ───────────────────────────────────────────────────────────────────
// Dated market-capitalisation statements (Wikidata P2226 + point-in-time
// P585 + unit) for up to 80 items. The client anchors 10-year price series
// on these to rebuild an absolute market-cap curve. Cached a week.
const CAP_UNITS = { Q4917: 'USD', Q4916: 'EUR', Q25344: 'CHF', Q25417: 'DKK', Q1104069: 'CAD', Q8146: 'JPY',
  Q25224: 'GBP', Q202040: 'KRW', Q31015: 'HKD', Q208526: 'TWD', Q39099: 'CNY', Q122922: 'SEK' };
async function handleCaps(reqUrl, ctx) {
  const ids = [...new Set((reqUrl.searchParams.get('ids') || '').split(',').filter(x => /^Q\d{1,10}$/.test(x)))].slice(0, 80).sort();
  if (!ids.length) return json({ error: 'no ids' }, 400);
  const key = new Request('https://cache/caps?ids=' + ids.join(','));
  const hit = await caches.default.match(key);
  if (hit) return withCors(hit);
  const q = `SELECT ?co ?cap ?date ?unit WHERE { VALUES ?co { ${ids.map(i => 'wd:' + i).join(' ')} }
    ?co p:P2226 ?st . ?st psv:P2226 [ wikibase:quantityAmount ?cap ; wikibase:quantityUnit ?unit ] . ?st pq:P585 ?date . }`;
  const r = await fetch('https://query.wikidata.org/sparql?query=' + encodeURIComponent(q), {
    headers: { Accept: 'application/sparql-results+json', 'User-Agent': 'wrld-news/1.0 (+https://github.com/Charloooot/My-News)' },
  });
  if (!r.ok) return json({ error: 'wikidata ' + r.status }, 502);
  const out = {};
  for (const b of (await r.json()).results.bindings) {
    const id = b.co.value.split('/').pop(), unit = CAP_UNITS[b.unit.value.split('/').pop()];
    if (!unit) continue;
    (out[id] ||= []).push({ date: b.date.value.slice(0, 10), cap: +b.cap.value, unit });
  }
  for (const v of Object.values(out)) v.sort((a, b) => a.date.localeCompare(b.date));
  const resp = new Response(JSON.stringify(out), { headers: { 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=604800', ...CORS } });
  ctx.waitUntil(caches.default.put(key, resp.clone()));
  return resp;
}

// ── /quotes ─────────────────────────────────────────────────────────────────
// Symbols are allow-listed by shape (letters, digits, . - ^ =) and capped, so
// this can't be turned into a generic proxy. Each symbol is cached separately.
async function handleQuotes(reqUrl, ctx) {
  const range = ['1d', '5d', '1mo', '5y', '10y'].includes(reqUrl.searchParams.get('range')) ? reqUrl.searchParams.get('range') : '5d';
  const long = range === '5y' || range === '10y';
  const interval = long ? '1mo' : range === '1mo' ? '1h' : range === '1d' ? '5m' : '15m';
  const symbols = [...new Set((reqUrl.searchParams.get('symbols') || '').split(',')
    .map(x => x.trim().toUpperCase()).filter(x => /^[A-Z0-9.^=-]{1,16}$/.test(x)))].slice(0, 30);
  if (!symbols.length) return json({ error: 'no symbols' }, 400);
  const cache = caches.default;
  const out = await Promise.all(symbols.map(async (sym) => {
    const key = new Request(`https://cache/quote?s=${encodeURIComponent(sym)}&r=${range}`);
    const hit = await cache.match(key);
    if (hit) return hit.json();
    try {
      const q = await fetchQuote(sym, range, interval);
      ctx.waitUntil(cache.put(key, new Response(JSON.stringify(q), {
        headers: { 'Content-Type': 'application/json', 'Cache-Control': `public, max-age=${long ? 43200 : range === '1d' ? 60 : 300}` },
      })));
      return q;
    } catch (e) {
      return { symbol: sym, error: String(e.message || e) };
    }
  }));
  return new Response(JSON.stringify({ range, quotes: out }), {
    headers: { 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=120', ...CORS },
  });
}

// ── /search ─────────────────────────────────────────────────────────────────
// Company lookup for MARKETS → SEARCH. Bounded query, cached a day per query.
async function handleSearch(reqUrl, ctx) {
  const q = (reqUrl.searchParams.get('q') || '').trim().toLowerCase().replace(/\s+/g, ' ');
  if (q.length < 2 || q.length > 60) return json({ error: 'q must be 2..60 characters' }, 400);
  const cache = caches.default;
  const key = new Request('https://cache/company-search?q=' + encodeURIComponent(q));
  const hit = await cache.match(key);
  if (hit) return withCors(hit);
  let quotes;
  try { quotes = await searchCompanies(q); }
  catch (e) { return json({ error: 'search unavailable', detail: String(e.message || e).slice(0, 120) }, 502); }
  const resp = new Response(JSON.stringify({ quotes }), {
    headers: { 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=86400' },
  });
  ctx.waitUntil(cache.put(key, resp.clone()));
  return withCors(resp);
}

// ── /gdelt ────────────────────────────────────────────────────────────────
// Edge-cached GDELT pass-through. The cache key is the normalized query, so all
// visitors asking the same thing share one upstream fetch.
async function handleGdelt(reqUrl) {
  const query = reqUrl.searchParams.get('query') || 'world OR politics';
  const maxrecords = Math.min(Number(reqUrl.searchParams.get('maxrecords')) || 250, 250);

  const upstream =
    'https://api.gdeltproject.org/api/v2/doc/doc' +
    `?query=${encodeURIComponent(query)}` +
    `&mode=ArtList&format=json&maxrecords=${maxrecords}&timespan=1d&sort=hybridrel`;

  const cache = caches.default;
  const cacheKey = new Request('https://cache/gdelt?q=' + encodeURIComponent(query) + '&n=' + maxrecords);
  const hit = await cache.match(cacheKey);
  if (hit) return withCors(hit);

  const r = await fetch(upstream, { headers: { 'User-Agent': 'wrld-news/1.0' } });
  const text = await r.text();
  const trimmed = text.trim();
  // GDELT returns plain-text throttle notices with HTTP 200 — treat as error so
  // we never cache a bad payload.
  if (!trimmed.startsWith('{')) {
    return json({ error: 'gdelt throttled', detail: trimmed.slice(0, 120), articles: [] }, 429);
  }

  const resp = new Response(trimmed, {
    headers: {
      'Content-Type': 'application/json',
      'Cache-Control': 'public, max-age=600',   // 10 min at the edge
      ...CORS,
    },
  });
  await cache.put(cacheKey, resp.clone());
  return resp;
}

// ── /extract ──────────────────────────────────────────────────────────────
// Readable article text. Tries jina reader first, then a raw-HTML paragraph
// scrape. Edge-cached 6h because article bodies don't change.
async function handleExtract(reqUrl) {
  const target = reqUrl.searchParams.get('url');
  if (!isPublicArticleUrl(target)) return json({ error: 'bad url' }, 400);

  const cache = caches.default;
  const cacheKey = new Request('https://cache/extract?u=' + encodeURIComponent(target));
  const hit = await cache.match(cacheKey);
  if (hit) return withCors(hit);

  let body = '';
  try {
    const r = await fetch('https://r.jina.ai/' + target, { headers: { Accept: 'text/plain' } });
    if (r.ok) {
      const t = await r.text();
      const i = t.indexOf('Markdown Content:');
      body = (i >= 0 ? t.slice(i + 'Markdown Content:'.length) : t).trim();
    }
  } catch (_) { /* fall through to raw scrape */ }

  if (body.length < 80) {
    try {
      const r = await fetch(target, { headers: { 'User-Agent': 'Mozilla/5.0 (wrld-news)' } });
      const html = (await r.text()).slice(0, MAX_HTML);
      body = scrapeParagraphs(html);
    } catch (_) { /* leave body empty */ }
  }

  if (body.length < 80) return json({ error: 'extraction failed' }, 422);

  const resp = new Response(body, {
    headers: {
      'Content-Type': 'text/plain; charset=utf-8',
      'Cache-Control': 'public, max-age=21600',  // 6h
      ...CORS,
    },
  });
  await cache.put(cacheKey, resp.clone());
  return resp;
}

// This endpoint fetches caller-supplied URLs, so keep it from being used as a
// general-purpose open proxy: http(s) on default ports, real hostnames only (no
// IP literals, localhost or internal suffixes). News article links always pass.
const MAX_HTML = 2_000_000;
function isPublicArticleUrl(raw) {
  let u;
  try { u = new URL(raw); } catch { return false; }
  if (u.protocol !== 'https:' && u.protocol !== 'http:') return false;
  if (u.port && u.port !== '80' && u.port !== '443') return false;
  if (u.username || u.password) return false;
  const h = u.hostname.toLowerCase();
  if (!h.includes('.') || h.startsWith('[')) return false;                 // bare names, IPv6
  if (/^\d{1,3}(\.\d{1,3}){3}$/.test(h)) return false;                       // IPv4 literal
  if (/(^|\.)(localhost|local|internal|lan|home|corp)$/.test(h)) return false;
  return true;
}

// Very small readability heuristic: strip non-content tags, keep <p> blocks.
function scrapeParagraphs(html) {
  const cleaned = html
    .replace(/<script[\s\S]*?<\/script>/gi, '')
    .replace(/<style[\s\S]*?<\/style>/gi, '')
    .replace(/<!--[\s\S]*?-->/g, '');
  const paras = [];
  const re = /<p[^>]*>([\s\S]*?)<\/p>/gi;
  let m;
  while ((m = re.exec(cleaned))) {
    const text = m[1].replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim();
    if (text.length > 60) paras.push(text);
  }
  return paras.join('\n\n').trim();
}

// ── /tle ────────────────────────────────────────────────────────────────────
// Edge-cached CelesTrak GP TLE pass-through. Cached 2h to match CelesTrak's own
// refresh cadence, which also keeps us under its per-IP repeat-download 403.
// `group` is allow-listed to alphanumerics + dash so it can't be used as an
// open proxy.
//
// The `active` group is stored in KV by its own cron and served from there:
// CelesTrak often leaves a live request from a Worker hanging for 20 s before a
// 522, and a request must not wait on that. TLEs stay usable for days, so one
// success every so often is enough, and the last good copy keeps being served.
const TLE_CRON = '29,59 * * * *';      // must match wrangler.toml
const TLE_KEY = 'tle:active';
const TLE_MIN_AGE_MS = 2 * 3600e3;     // CelesTrak updates the data every 2 h
const TLE_MIN_BYTES = 500000;          // the full group is about 3 MB

async function refreshTle(env) {
  if (!env.NEWS) return;
  const at = Number(await env.NEWS.get(TLE_KEY + ':at')) || 0;
  if (Date.now() - at < TLE_MIN_AGE_MS) return;
  const r = await fetch('https://celestrak.org/NORAD/elements/gp.php?GROUP=active&FORMAT=tle', {
    headers: { 'User-Agent': 'wrld-news/1.0' }, signal: AbortSignal.timeout(25000),
  });
  const text = await r.text();
  // Refusals and errors come back as short plain text: never store those.
  if (!r.ok || text.length < TLE_MIN_BYTES || !text.includes('\n1 25544')) {
    throw new Error(`celestrak ${r.status}: ${text.slice(0, 80)}`);
  }
  await env.NEWS.put(TLE_KEY, text);
  await env.NEWS.put(TLE_KEY + ':at', String(Date.now()));
}

async function handleTle(reqUrl, env, ctx) {
  let group = (reqUrl.searchParams.get('group') || 'active').toLowerCase();
  if (!/^[a-z0-9-]{2,32}$/.test(group)) group = 'active';

  if (group === 'active' && env.NEWS) {
    const key = new Request('https://cache/tle-kv?g=active');
    const hit = await caches.default.match(key);
    if (hit) return withCors(hit);
    const text = await env.NEWS.get(TLE_KEY);
    if (!text) return json({ error: 'no stored TLE yet' }, 503);   // answer fast: the page has its own fallbacks
    const resp = new Response(text, {
      headers: { 'Content-Type': 'text/plain; charset=utf-8', 'Cache-Control': 'public, max-age=1800', ...CORS },
    });
    ctx.waitUntil(caches.default.put(key, resp.clone()));
    return resp;
  }

  const upstream =
    `https://celestrak.org/NORAD/elements/gp.php?GROUP=${group}&FORMAT=tle`;

  const cache = caches.default;
  const cacheKey = new Request('https://cache/tle?g=' + group);
  const hit = await cache.match(cacheKey);
  if (hit) return withCors(hit);

  const r = await fetch(upstream, { headers: { 'User-Agent': 'wrld-news/1.0' } });
  const text = await r.text();
  // CelesTrak signals the per-IP repeat-download limit with a 403 + plain-text
  // body; only cache a payload that actually contains TLE lines.
  if (!/\b1 \d{5}/.test(text)) {
    return json({ error: 'celestrak unavailable', detail: text.slice(0, 120) }, 502);
  }

  const resp = new Response(text, {
    headers: {
      'Content-Type': 'text/plain; charset=utf-8',
      'Cache-Control': 'public, max-age=7200',   // 2h at the edge
      ...CORS,
    },
  });
  await cache.put(cacheKey, resp.clone());
  return resp;
}

// ── /satcat ───────────────────────────────────────────────────────────────
// Edge-cached SATCAT (NORAD_CAT_ID → OWNER). Changes rarely, so cache 24h.
async function handleSatcat() {
  const upstream = 'https://celestrak.org/pub/satcat.csv';
  const cache = caches.default;
  const cacheKey = new Request('https://cache/satcat');
  const hit = await cache.match(cacheKey);
  if (hit) return withCors(hit);

  const r = await fetch(upstream, { headers: { 'User-Agent': 'wrld-news/1.0' } });
  const text = await r.text();
  if (!text.includes('NORAD_CAT_ID')) {
    return json({ error: 'satcat unavailable' }, 502);
  }

  const resp = new Response(text, {
    headers: {
      'Content-Type': 'text/csv; charset=utf-8',
      'Cache-Control': 'public, max-age=86400',  // 24h
      ...CORS,
    },
  });
  await cache.put(cacheKey, resp.clone());
  return resp;
}

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json', ...CORS },
  });
}

function withCors(resp) {
  const h = new Headers(resp.headers);
  for (const [k, v] of Object.entries(CORS)) h.set(k, v);
  return new Response(resp.body, { status: resp.status, headers: h });
}
