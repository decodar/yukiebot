import fs from 'node:fs';

const API_KEY = process.env.APOLLO_API_KEY;
if (!API_KEY) {
  console.error('Missing APOLLO_API_KEY env var');
  process.exit(1);
}

const BASE = 'https://api.apollo.io/v1';

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

function ownerSignal(org) {
  const txt = [org.name, org.industry, ...(org.keywords || [])].filter(Boolean).join(' ').toLowerCase();
  const signals = ['founder', 'owner', 'private', 'family'];
  return signals.filter(s => txt.includes(s)).join(', ');
}

function csvEscape(value) {
  const s = String(value ?? '');
  if (/[",\n]/.test(s)) return `"${s.replaceAll('"', '""')}"`;
  return s;
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
];

const fields = [
  'Company name', 'Website', 'HQ city (BC)', 'Employee range',
  'Ownership/management signal', 'Open jobs count/hiring signal',
  'CEO or manager name', 'CEO/manager title', 'Email address', 'Telephone number',
  'LinkedIn / Apollo URL', 'Notes / confidence'
];

async function run() {
  const all = [];
  const seen = new Set();

  for (const loc of BC_CITIES) {
    const payload = {
      page: 1,
      per_page: 25,
      organization_locations: [loc],
      organization_num_employees_ranges: ['25,50', '51,100', '101,200', '201,500'],
    };
    const j = await post('/organizations/search', payload);
    for (const o of (j.organizations || [])) {
      if (seen.has(o.id)) continue;
      seen.add(o.id);
      all.push(o);
    }
    if (all.length >= 200) break;
  }

  const strictBC = all.filter(o => {
    const state = norm(o.state);
    const country = norm(o.country);
    const city = first(o.city);
    const employees = Number(o.estimated_num_employees || 0);
    const inBC = state === 'bc' || state === 'british columbia';
    const inCA = country === 'canada' || country === 'ca';
    const empOk = employees >= 25 && employees <= 250;
    return inBC && inCA && empOk && city;
  });

  // Hiring proxy due API access limits: positive 6- or 12-month headcount growth.
  const likelyHiring = strictBC.filter(o =>
    Number(o.organization_headcount_six_month_growth || 0) > 0 ||
    Number(o.organization_headcount_twelve_month_growth || 0) > 0
  );

  const chosen = (likelyHiring.length >= 10 ? likelyHiring : strictBC).slice(0, 10);

  const rows = [];
  for (const o of chosen) {
    let personName = '', personTitle = '', personEmail = '', personPhone = '';
    try {
      const contacts = await post('/contacts/search', {
        page: 1,
        per_page: 3,
        q_organization_domains: [first(o.primary_domain)],
      });
      const c = (contacts.contacts || [])[0];
      if (c) {
        personName = first(c.name, `${first(c.first_name)} ${first(c.last_name)}`.trim());
        personTitle = first(c.title);
        personEmail = first(c.email);
        personPhone = first(c.phone_numbers?.[0]?.sanitized_number, c.phone_numbers?.[0]?.raw_number, c.sanitized_phone);
      }
    } catch {}

    rows.push({
      'Company name': first(o.name),
      'Website': first(o.website_url, o.primary_domain),
      'HQ city (BC)': `${first(o.city)}, ${first(o.state)}`,
      'Employee range': first(o.estimated_num_employees),
      'Ownership/management signal': ownerSignal(o),
      'Open jobs count/hiring signal': 'Likely hiring (headcount growth proxy)',
      'CEO or manager name': personName,
      'CEO/manager title': personTitle,
      'Email address': personEmail,
      'Telephone number': personPhone || first(o.phone),
      'LinkedIn / Apollo URL': first(o.linkedin_url),
      'Notes / confidence': 'Higher on BC/size; hiring is proxy; contact data depends on plan',
    });
  }

  const lines = [fields.join(',')];
  for (const row of rows) lines.push(fields.map(f => csvEscape(row[f])).join(','));
  fs.writeFileSync('bc_companies_10.csv', lines.join('\n'), 'utf8');
  console.log(`Wrote ${rows.length} rows to bc_companies_10.csv`);
}

run().catch(err => {
  console.error(err.message || err);
  process.exit(1);
});
