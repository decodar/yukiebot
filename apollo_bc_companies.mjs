import fs from 'node:fs';

const API_KEY = process.env.APOLLO_API_KEY;
if (!API_KEY) {
  console.error('Missing APOLLO_API_KEY env var');
  process.exit(1);
}

const BASE = 'https://api.apollo.io/api/v1';

async function post(path, payload) {
  const res = await fetch(`${BASE}${path}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Api-Key': API_KEY,
    },
    body: JSON.stringify(payload),
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`${res.status} ${path}\n${text.slice(0, 1000)}`);
  return text ? JSON.parse(text) : {};
}

const first = (...vals) => vals.find(v => v !== undefined && v !== null && v !== '') ?? '';

function ownerSignal(org) {
  const txt = [org.short_description, org.keywords, org.name].filter(Boolean).join(' ').toLowerCase();
  const signals = ['founder-led', 'founder', 'owner', 'privately held', 'family-owned', 'entrepreneur'];
  return signals.filter(s => txt.includes(s)).join(', ');
}

function csvEscape(value) {
  const s = String(value ?? '');
  if (/[",\n]/.test(s)) return `"${s.replaceAll('"', '""')}"`;
  return s;
}

const companyPayload = {
  page: 1,
  per_page: 50,
  organization_num_employees_ranges: ['25,50', '51,100', '101,200', '201,500'],
  organization_locations: ['British Columbia, Canada']
};

const fields = [
  'Company name', 'Website', 'HQ city (BC)', 'Employee range',
  'Ownership/management signal', 'Open jobs count/hiring signal',
  'CEO or manager name', 'CEO/manager title', 'Email address', 'Telephone number',
  'LinkedIn / Apollo URL', 'Notes / confidence'
];

async function run() {
  const search = await post('/mixed_companies/search', companyPayload);
  const orgs = search.organizations || search.accounts || [];

  let filtered = orgs.filter(o =>
    o.is_hiring === true || (o.job_postings_count || 0) > 0 || (o.active_job_count || 0) > 0
  );
  if (filtered.length < 10) filtered = orgs.slice(0, 30);

  const rows = [];
  const seen = new Set();

  for (const o of filtered) {
    const orgId = o.id || o.organization_id;
    if (!orgId || seen.has(orgId)) continue;
    seen.add(orgId);

    let personName = '', personTitle = '', personEmail = '', personPhone = '';
    try {
      const peoplePayload = {
        page: 1,
        per_page: 5,
        organization_ids: [orgId],
        person_titles: ['CEO', 'Chief Executive Officer', 'Founder', 'Owner', 'General Manager', 'Managing Director', 'President'],
      };
      const p = await post('/mixed_people/search', peoplePayload);
      const people = p.people || p.contacts || [];
      if (people.length) {
        const person = people[0];
        personName = first(person.name, `${first(person.first_name)} ${first(person.last_name)}`.trim());
        personTitle = first(person.title);
        personEmail = first(person.email, person.work_email);
        personPhone = first(person.phone, person.phone_number, person.mobile_phone);
      }
    } catch {}

    const hq = [first(o.city, o.organization_city), first(o.state, o.organization_state)].filter(Boolean).join(', ');

    rows.push({
      'Company name': first(o.name),
      'Website': first(o.website_url, o.primary_domain),
      'HQ city (BC)': hq,
      'Employee range': first(o.estimated_num_employees, o.organization_num_employees),
      'Ownership/management signal': ownerSignal(o),
      'Open jobs count/hiring signal': first(o.job_postings_count, o.active_job_count, o.is_hiring),
      'CEO or manager name': personName,
      'CEO/manager title': personTitle,
      'Email address': personEmail,
      'Telephone number': personPhone,
      'LinkedIn / Apollo URL': first(o.linkedin_url, o.organization_linkedin_url, o.apollo_url, o.organization_apollo_url),
      'Notes / confidence': 'Medium (API field availability varies by org/profile)',
    });

    if (rows.length >= 10) break;
  }

  const lines = [fields.join(',')];
  for (const row of rows) {
    lines.push(fields.map(f => csvEscape(row[f])).join(','));
  }
  fs.writeFileSync('bc_companies_10.csv', lines.join('\n'), 'utf8');
  console.log(`Wrote ${rows.length} rows to bc_companies_10.csv`);
}

run().catch(err => {
  console.error(err.message || err);
  process.exit(1);
});
