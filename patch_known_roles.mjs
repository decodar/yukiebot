import fs from 'node:fs';

const files = ['bc_companies_top25_enriched.csv', 'bc_companies_100_more_enriched.csv'];

function parseCsv(content) {
  const lines = content.split(/\r?\n/).filter(Boolean);
  const parseLine = (line) => {
    const out = [];
    let cur = '';
    let q = false;
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (ch === '"') {
        if (q && line[i + 1] === '"') { cur += '"'; i++; }
        else q = !q;
      } else if (ch === ',' && !q) { out.push(cur); cur = ''; }
      else cur += ch;
    }
    out.push(cur);
    return out;
  };
  const headers = parseLine(lines[0]);
  const rows = lines.slice(1).map((line) => {
    const vals = parseLine(line);
    const row = {};
    headers.forEach((h, i) => row[h] = vals[i] ?? '');
    return row;
  });
  return { headers, rows };
}

function toCsv(headers, rows) {
  const esc = (v) => {
    const s = String(v ?? '');
    return /[",\n]/.test(s) ? `"${s.replaceAll('"', '""')}"` : s;
  };
  return [headers.join(','), ...rows.map(r => headers.map(h => esc(r[h] ?? '')).join(','))].join('\n');
}

const herschelJobs = [
  { jobID: '4661000006', jobName: 'SALES OPERATIONS SPECIALIST', department: 'Logistics, Distribution & Sales Ops', location: 'Vancouver', employmentType: '', postedAt: '2026-03-10T12:58:24-04:00', applyUrl: 'https://herschel.com/careers?gh_jid=4661000006' },
  { jobID: '4649330006', jobName: 'ASSISTANT STORE MANAGER', department: 'Retail', location: 'Richmond', employmentType: '', postedAt: '2026-03-06T20:14:10-05:00', applyUrl: 'https://herschel.com/careers?gh_jid=4649330006' },
  { jobID: '4658352006', jobName: 'SALES ASSOCIATE (PART-TIME)', department: 'Retail', location: 'Burnaby', employmentType: 'Part-Time', postedAt: '2026-03-06T20:14:10-05:00', applyUrl: 'https://herschel.com/careers?gh_jid=4658352006' },
  { jobID: '4661956006', jobName: 'SALES ASSOCIATE (PART-TIME)', department: 'Retail', location: 'Robson Street, Vancouver', employmentType: 'Part-Time', postedAt: '2026-03-10T16:54:24-04:00', applyUrl: 'https://herschel.com/careers?gh_jid=4661956006' },
  { jobID: '4657174006', jobName: 'SALES ASSOCIATE (SEASONAL)', department: 'Retail', location: 'Gastown, Vancouver', employmentType: 'Seasonal', postedAt: '2026-03-06T20:14:10-05:00', applyUrl: 'https://herschel.com/careers?gh_jid=4657174006' }
];

const dapperJobs = [
  { jobID: '9835', jobName: 'Design Engineer', department: 'Engineering (R&D)', location: 'United States; Vancouver, British Columbia, Canada', employmentType: 'Full Time', postedAt: '', applyUrl: 'https://careers.kula.ai/dapperlabs/9835/?jobs=true' }
];

const cymaxJobs = [
  { jobID: 'Business Development Manager (eCommerce)', jobName: 'Business Development Manager (eCommerce)', department: '', location: 'Hybrid, Vancouver, BC, CA', employmentType: 'Full Time', postedAt: 'Today', applyUrl: 'https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html?ccId=19000101_000001&cid=33a9306c-011d-4f55-84d4-d407c2ff126d&lang=en_CA' },
  { jobID: 'Account Manager (eCommerce)', jobName: 'Account Manager (eCommerce)', department: '', location: 'Hybrid, Vancouver, BC, CA', employmentType: 'Full Time', postedAt: 'Today', applyUrl: 'https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html?ccId=19000101_000001&cid=33a9306c-011d-4f55-84d4-d407c2ff126d&lang=en_CA' },
  { jobID: 'Director, Partnerships & B2B (eCommerce)', jobName: 'Director, Partnerships & B2B (eCommerce)', department: '', location: 'Hybrid, Vancouver, BC, CA', employmentType: 'Full Time', postedAt: '12 days ago', applyUrl: 'https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html?ccId=19000101_000001&cid=33a9306c-011d-4f55-84d4-d407c2ff126d&lang=en_CA' },
  { jobID: 'Senior eCommerce Marketing Analyst', jobName: 'Senior eCommerce Marketing Analyst', department: '', location: 'Vancouver, BC, CA', employmentType: 'Full Time', postedAt: '12 days ago', applyUrl: 'https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html?ccId=19000101_000001&cid=33a9306c-011d-4f55-84d4-d407c2ff126d&lang=en_CA' },
  { jobID: 'Senior Product Manager, AI (eCommerce)', jobName: 'Senior Product Manager, AI (eCommerce)', department: '', location: 'Hybrid, Vancouver, BC, CA', employmentType: 'Full Time', postedAt: '12 days ago', applyUrl: 'https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html?ccId=19000101_000001&cid=33a9306c-011d-4f55-84d4-d407c2ff126d&lang=en_CA' },
  { jobID: 'Director, eCommerce Operations', jobName: 'Director, eCommerce Operations', department: '', location: 'Hybrid, Vancouver, BC, CA', employmentType: 'Full Time', postedAt: '14 days ago', applyUrl: 'https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html?ccId=19000101_000001&cid=33a9306c-011d-4f55-84d4-d407c2ff126d&lang=en_CA' },
  { jobID: 'Director, Analytics & Insights - Freight Club', jobName: 'Director, Analytics & Insights - Freight Club', department: '', location: 'Hybrid, Vancouver, BC, CA', employmentType: 'Full Time', postedAt: '15 days ago', applyUrl: 'https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html?ccId=19000101_000001&cid=33a9306c-011d-4f55-84d4-d407c2ff126d&lang=en_CA' },
  { jobID: 'Business Development Manager (Freight Club)', jobName: 'Business Development Manager (Freight Club)', department: '', location: 'Hybrid, Vancouver, BC, CA', employmentType: 'Full Time', postedAt: '27 days ago', applyUrl: 'https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html?ccId=19000101_000001&cid=33a9306c-011d-4f55-84d4-d407c2ff126d&lang=en_CA' }
];

for (const file of files) {
  if (!fs.existsSync(file)) continue;
  const { headers, rows } = parseCsv(fs.readFileSync(file, 'utf8'));
  for (const r of rows) {
    const name = (r['Company name'] || '').toLowerCase();
    if (name === 'herschel supply company') {
      r['Careers URL'] = 'https://herschel.com/careers';
      r['Resolved Careers URL'] = 'https://herschel.com/careers';
      r['Open Roles Count'] = '5';
      r['Jobs JSON'] = JSON.stringify(herschelJobs);
    }
    if (name === 'dapper labs') {
      r['Careers URL'] = 'https://www.dapperlabs.com/careers';
      r['Resolved Careers URL'] = 'https://www.dapperlabs.com/careers';
      r['Open Roles Count'] = '1';
      r['Jobs JSON'] = JSON.stringify(dapperJobs);
    }
    if (name === 'cymax group technologies') {
      r['Careers URL'] = 'https://www.cymaxgroup.com/careers';
      r['Resolved Careers URL'] = 'https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html?ccId=19000101_000001&cid=33a9306c-011d-4f55-84d4-d407c2ff126d&lang=en_CA';
      r['Open Roles Count'] = '8';
      r['Jobs JSON'] = JSON.stringify(cymaxJobs);
    }
  }
  fs.writeFileSync(file, toCsv(headers, rows), 'utf8');
  console.log('Patched', file);
}
