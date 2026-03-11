import fs from 'node:fs';

const API_KEY = process.env.APOLLO_API_KEY;
if (!API_KEY) {
  console.error('Missing APOLLO_API_KEY env var');
  process.exit(1);
}

const BASE = 'https://api.apollo.io/v1';
const TARGET_COUNT = 100;

async function post(path, payload) {
  const res = await fetch(`${BASE}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Api-Key': API_KEY },
    body: JSON.stringify(payload),
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`${res.status} ${path}\n${text.slice(0, 1000)}`);
  return text ? JSON.parse(text) : {};
}

const first = (...vals) => vals.find(v => v !== undefined && v !== null && v !== '') ?? '';
const norm = (s) => String(s || '').toLowerCase().trim();

function csvEscape(value) {
  const s = String(value ?? '');
  if (/[",\n]/.test(s)) return `"${s.replaceAll('"', '""')}"`;
  return s;
}

function ownerSignal(org) {
  const txt = [org.name, org.industry, ...(org.keywords || [])].filter(Boolean).join(' ').toLowerCase();
  const signals = ['founder', 'owner', 'private', 'family'];
  return signals.filter(s => txt.includes(s)).join(', ');
}

const BC_CITIES = [
  'Vancouver, British Columbia, Canada',
  'Victoria, British Columbia, Canada',
  'Burnaby, British Columbia, Canada',
  'Surrey, British Columbia, Canada',
  'Richmond, British Columbia, Canada',
  'Kelowna, British Columbia, Canada',
  'Coquitlam, British Columbia, Canada',
  'Langley, British Columbia, Canada',
  'North Vancouver, British Columbia, Canada',
  'Abbotsford, British Columbia, Canada',
  'Nanaimo, British Columbia, Canada',
  'Kamloops, British Columbia, Canada',
  'Prince George, British Columbia, Canada',
  'Chilliwack, British Columbia, Canada',
  'Delta, British Columbia, Canada',
  'New Westminster, British Columbia, Canada',
  'Maple Ridge, British Columbia, Canada',
  'Penticton, British Columbia, Canada',
  'Vernon, British Columbia, Canada',
  'Nanaimo, British Columbia, Canada'
];

const fields = [
  'Company name', 'Website', 'HQ city (BC)', 'Employee range',
  'Ownership/management signal', 'Open jobs count/hiring signal',
  'CEO or manager name', 'CEO/manager title', 'Email address', 'Telephone number',
  'LinkedIn / Apollo URL', 'Notes / confidence'
];

function readExistingNames(path) {
  if (!fs.existsSync(path)) return new Set();
  const txt = fs.readFileSync(path, 'utf8');
  const lines = txt.split(/\r?\n/).filter(Boolean);
  if (lines.length <= 1) return new Set();
  const set = new Set();
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    const firstComma = line.indexOf(',');
    if (firstComma > 0) set.add(line.slice(0, firstComma).replace(/^"|"$/g, '').trim().toLowerCase());
  }
  return set;
}

async function run() {
  const exclude = readExistingNames('bc_companies_10.csv');
  const collected = new Map();

  for (const loc of BC_CITIES) {
    for (let page = 1; page <= 8; page++) {
      const payload = {
        page,
        per_page: 100,
        organization_locations: [loc],
        organization_num_employees_ranges: ['25,50', '51,100', '101,200', '201,500'],
      };

      let j;
      try {
        j = await post('/organizations/search', payload);
      } catch {
        continue;
      }

      const orgs = j.organizations || [];
      if (!orgs.length) break;

      for (const o of orgs) {
        if (!o?.id || collected.has(o.id)) continue;

        const state = norm(o.state);
        const country = norm(o.country);
        const employees = Number(o.estimated_num_employees || 0);
        const inBC = state === 'bc' || state === 'british columbia';
        const inCA = country === 'canada' || country === 'ca';
        const empOk = employees >= 25 && employees <= 250;
        const nameKey = norm(o.name);
        if (!inBC || !inCA || !empOk || !nameKey || exclude.has(nameKey)) continue;

        const likelyHiring = Number(o.organization_headcount_six_month_growth || 0) > 0 ||
          Number(o.organization_headcount_twelve_month_growth || 0) > 0;

        collected.set(o.id, {
          'Company name': first(o.name),
          'Website': first(o.website_url, o.primary_domain),
          'HQ city (BC)': `${first(o.city)}, ${first(o.state)}`,
          'Employee range': first(o.estimated_num_employees),
          'Ownership/management signal': ownerSignal(o),
          'Open jobs count/hiring signal': likelyHiring ? 'Likely hiring (headcount growth proxy)' : 'Unknown (no hiring endpoint access)',
          'CEO or manager name': '',
          'CEO/manager title': '',
          'Email address': '',
          'Telephone number': first(o.phone),
          'LinkedIn / Apollo URL': first(o.linkedin_url),
          'Notes / confidence': 'BC/size verified; hiring is proxy; people/contact fields limited by API access',
          _score: likelyHiring ? 1 : 0,
        });
      }

      if (collected.size >= TARGET_COUNT * 3) break;
    }
    if (collected.size >= TARGET_COUNT * 3) break;
  }

  const picked = [...collected.values()]
    .sort((a, b) => b._score - a._score)
    .slice(0, TARGET_COUNT)
    .map(({ _score, ...row }) => row);

  const lines = [fields.join(',')];
  for (const row of picked) lines.push(fields.map(f => csvEscape(row[f])).join(','));

  const out = 'bc_companies_100_more.csv';
  fs.writeFileSync(out, lines.join('\n'), 'utf8');
  console.log(`Wrote ${picked.length} rows to ${out}`);
}

run().catch(err => {
  console.error(err.message || err);
  process.exit(1);
});
