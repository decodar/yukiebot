import fs from 'node:fs';

const FILE = 'bc_companies_top25_enriched.csv';

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
      } else if (ch === ',' && !q) {
        out.push(cur); cur = '';
      } else cur += ch;
    }
    out.push(cur);
    return out;
  };

  const headers = parseLine(lines[0]);
  const rows = lines.slice(1).map((line) => {
    const vals = parseLine(line);
    const r = {};
    headers.forEach((h, i) => (r[h] = vals[i] ?? ''));
    return r;
  });

  return { headers, rows };
}

const esc = (v) => {
  const s = String(v ?? '');
  return /[",\n]/.test(s) ? `"${s.replaceAll('"', '""')}"` : s;
};

function normalizeExistingJobsJson(raw) {
  if (!raw) return '[]';
  try {
    const arr = JSON.parse(raw);
    if (!Array.isArray(arr)) return '[]';
    const mapped = arr.map((j) => ({
      jobID: j.jobID || j.id || '',
      jobName: j.jobName || j.title || '',
      department: j.department || '',
      location: j.location || '',
      employmentType: j.employmentType || '',
      postedAt: j.postedAt || '',
      applyUrl: j.applyUrl || j.url || j.jobID || ''
    }));
    return JSON.stringify(mapped);
  } catch {
    return '[]';
  }
}

async function run() {
  const { headers, rows } = parseCsv(fs.readFileSync(FILE, 'utf8'));

  if (!headers.includes('Resolved Careers URL')) {
    const idx = headers.indexOf('Careers URL');
    if (idx >= 0) headers.splice(idx + 1, 0, 'Resolved Careers URL');
    else headers.push('Resolved Careers URL');
  }

  for (const row of rows) {
    row['Resolved Careers URL'] = row['Careers URL'] || row['Resolved Careers URL'] || '';
    row['Jobs JSON'] = normalizeExistingJobsJson(row['Jobs JSON']);

    if ((row['Company name'] || '').toLowerCase() === 'herschel supply company') {
      row['Resolved Careers URL'] = 'https://herschel.com/careers';
      const herschelJobs = [
        { jobID: '4661000006', jobName: 'SALES OPERATIONS SPECIALIST', department: 'Logistics, Distribution & Sales Ops', location: 'Vancouver', employmentType: '', postedAt: '2026-03-10T12:58:24-04:00', applyUrl: 'https://herschel.com/careers?gh_jid=4661000006' },
        { jobID: '4649330006', jobName: 'ASSISTANT STORE MANAGER', department: 'Retail', location: 'Richmond', employmentType: '', postedAt: '2026-03-06T20:14:10-05:00', applyUrl: 'https://herschel.com/careers?gh_jid=4649330006' },
        { jobID: '4658352006', jobName: 'SALES ASSOCIATE (PART-TIME)', department: 'Retail', location: 'Burnaby', employmentType: 'Part-Time', postedAt: '2026-03-06T20:14:10-05:00', applyUrl: 'https://herschel.com/careers?gh_jid=4658352006' },
        { jobID: '4661956006', jobName: 'SALES ASSOCIATE (PART-TIME)', department: 'Retail', location: 'Robson Street, Vancouver', employmentType: 'Part-Time', postedAt: '2026-03-10T16:54:24-04:00', applyUrl: 'https://herschel.com/careers?gh_jid=4661956006' },
        { jobID: '4657174006', jobName: 'SALES ASSOCIATE (SEASONAL)', department: 'Retail', location: 'Gastown, Vancouver', employmentType: 'Seasonal', postedAt: '2026-03-06T20:14:10-05:00', applyUrl: 'https://herschel.com/careers?gh_jid=4657174006' }
      ];
      row['Open Roles Count'] = String(herschelJobs.length);
      row['Jobs JSON'] = JSON.stringify(herschelJobs);
    }
  }

  const lines = [headers.join(',')];
  for (const row of rows) lines.push(headers.map((h) => esc(row[h] ?? '')).join(','));
  fs.writeFileSync(FILE, lines.join('\n'), 'utf8');
  console.log('Updated', FILE);
}

run();
