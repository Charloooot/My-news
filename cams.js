/**
 * Open camera directories — compact indexes for the CAMERAS tab.
 *
 * Only official, intentionally public cameras (road-traffic / road-weather
 * agencies publishing open data). Never unsecured private cameras.
 *
 * Most sources are fetched by the browser directly (they send CORS headers).
 * Some need the Worker:
 *   /cams/caltrans?d=1..12  Caltrans publishes ~12 MB of uncompressed JSON per
 *                           state; we shrink one district (≤2.5 MB) to ~150 KB.
 *   /cams/nz                NZTA's JSON only allows CORS for localhost.
 *   /cams/bc                DriveBC's JSON sends no CORS header.
 * Both are edge-cached for an hour (camera lists change rarely; the images
 * themselves are loaded straight from the agencies).
 *
 * Compact row: [id, name, lat, lon, imageUrl, liveVideoUrl|'' , direction|'']
 */

const UA = 'wrld-news/1.0 (+open camera index)';

export async function caltransDistrict(d) {
  const dd = String(d).padStart(2, '0');
  const r = await fetch(`https://cwwp2.dot.ca.gov/data/d${d}/cctv/cctvStatusD${dd}.json`, { headers: { 'User-Agent': UA } });
  if (!r.ok) throw new Error('caltrans ' + r.status);
  const j = await r.json();
  const out = [];
  for (const e of j.data || []) {
    const c = e.cctv;
    if (!c || c.inService !== 'true') continue;
    const l = c.location || {};
    const img = c.imageData?.static?.currentImageURL;
    const lat = +l.latitude, lon = +l.longitude;
    if (!img || !Number.isFinite(lat) || !Number.isFinite(lon)) continue;
    out.push([`ca${d}-${c.index}`, l.locationName || l.nearbyPlace || 'Caltrans camera', +lat.toFixed(5), +lon.toFixed(5),
      img, c.imageData?.streamingVideoURL || '', l.direction || '']);
  }
  return out;
}

export async function nzCameras() {
  const r = await fetch('https://www.journeys.nzta.govt.nz/assets/map-data-cache/cameras.json', { headers: { 'User-Agent': UA } });
  if (!r.ok) throw new Error('nzta ' + r.status);
  const j = await r.json();
  const out = [];
  for (const f of j.features || []) {
    const p = f.properties || {};
    if (p.Offline || p.UnderMaintenance || !p.ImageUrl) continue;
    const lat = +p.Latitude, lon = +p.Longitude;
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;
    out.push([`nz-${p.ExternalId ?? p.id}`, p.Name || 'NZTA camera', +lat.toFixed(5), +lon.toFixed(5), p.ImageUrl, '', p.Direction || '']);
  }
  return out;
}

// ── Spain · DGT (DATEX II device publication, ~2,000 cameras) ──────────────
// 3.7 MB of XML; split per device and pull a few fields with precompiled
// regexes (~4 ms) — no XML parser needed in the Worker.
const RX = {
  type: /<ns2:typeOfDevice>camera</, id: /id="(\d+)"/, url: /<fse:deviceUrl>([^<]+)</,
  lat: /<loc:latitude>([-\d.]+)</, lon: /<loc:longitude>([-\d.]+)</, road: /<loc:roadName>([^<]*)</,
  km: /<lse:kilometerPoint>([^<]*)</, dest: /<loc:roadDestination>([^<]*)</, prov: /<lse:province>([^<]*)</,
};
export async function spainCameras() {
  const r = await fetch('https://nap.dgt.es/datex2/v3/dgt/DevicePublication/camaras_datex2_v36.xml', { headers: { 'User-Agent': UA } });
  if (!r.ok) throw new Error('dgt ' + r.status);
  const xml = await r.text();
  const g = (b, re) => (b.match(re) || [])[1] || '';
  const out = [];
  for (const b of xml.split('<ns2:device ').slice(1)) {
    if (!RX.type.test(b)) continue;
    const url = g(b, RX.url), lat = +g(b, RX.lat), lon = +g(b, RX.lon);
    if (!url || !lat || !lon) continue;
    const road = g(b, RX.road), km = g(b, RX.km), dest = g(b, RX.dest);
    out.push([`es-${g(b, RX.id)}`, [road, km && `km ${km}`, dest && `→ ${dest}`].filter(Boolean).join(' ') || 'DGT camera',
      lat, lon, url, '', g(b, RX.prov)]);
  }
  return out;
}

// ── Iceland · Vegagerðin (road administration) ─────────────────────────────
export async function icelandCameras() {
  const r = await fetch('https://gagnaveita.vegagerdin.is/api/vefmyndavelar2014_1', { headers: { 'User-Agent': UA } });
  if (!r.ok) throw new Error('vegagerdin ' + r.status);
  const out = [];
  for (const c of await r.json()) {
    if (!c.Slod || !Number.isFinite(c.Breidd) || !Number.isFinite(c.Lengd)) continue;
    out.push([`is-${c.Slod.split('/').pop().replace(/\.\w+$/, '')}`, `${c.Myndavel} — ${c.Skyring || c.Vegheiti || ''}`.replace(/ — $/, ''),
      c.Breidd, c.Lengd, c.Slod, '', c.Vegheiti || '']);
  }
  return out;
}

// ── British Columbia · DriveBC (BC HighwayCams, ~1,050 cameras) ────────────
// Published under the Open Government Licence – British Columbia (BC Data
// Catalogue record "BC HighwayCams"). The JSON has no CORS header, so it is
// served through the Worker; the images load straight from drivebc.ca.
export async function bcCameras() {
  const r = await fetch('https://www.drivebc.ca/api/webcams/', { headers: { 'User-Agent': UA } });
  if (!r.ok) throw new Error('drivebc ' + r.status);
  const out = [];
  for (const c of await r.json()) {
    const [lon, lat] = c.location?.coordinates || [];
    const img = c.links?.imageDisplay;
    if (!c.is_on || c.should_appear === false || !img || !Number.isFinite(lat) || !Number.isFinite(lon)) continue;
    out.push([`bc-${c.id}`, c.caption || c.name || 'DriveBC camera', +lat.toFixed(5), +lon.toFixed(5),
      'https://www.drivebc.ca' + img.split('?')[0], '', [c.highway_display && `Hwy ${c.highway_display}`, c.orientation].filter(Boolean).join(' · ')]);
  }
  return out;
}

// ── US National Park Service webcams (public landmarks) ────────────────────
// Many NPS webcams are viewed on nps.gov pages rather than as a raw image, so
// rows carry the page URL (index 7); image (index 4) may be a still preview.
export async function npsWebcams(apiKey = 'DEMO_KEY') {
  const r = await fetch(`https://developer.nps.gov/api/v1/webcams?limit=600&api_key=${encodeURIComponent(apiKey)}`, { headers: { 'User-Agent': UA } });
  if (!r.ok) throw new Error('nps ' + r.status);
  const out = [];
  for (const w of (await r.json()).data || []) {
    const lat = +w.latitude, lon = +w.longitude;
    if (w.status !== 'Active' || !lat || !lon || !w.url) continue;
    out.push([`nps-${w.id}`, w.title, +lat.toFixed(5), +lon.toFixed(5), w.images?.[0]?.url || '', '',
      (w.relatedParks || []).map(p => p.fullName).join(', '), w.url, w.isStreaming ? 1 : 0]);
  }
  return out;
}
