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
    const r = {};
    headers.forEach((h, i) => r[h] = vals[i] ?? '');
    return r;
  });
  return rows;
}

function isNoisyJob(j) {
  const t = `${j.jobName || ''} ${j.jobID || ''}`.toLowerCase();
  if (!j.jobName || j.jobName.length < 3) return true;
  if (t.includes('{') || t.includes('}') || t.includes('var(--') || t.includes('.fe-')) return true;
  if (/privacy|cookie|terms|board of directors|member directory|product security|landing pages/i.test(t)) return true;
  return false;
}

const flagged = [];
for (const f of files) {
  if (!fs.existsSync(f)) continue;
  const rows = parseCsv(fs.readFileSync(f, 'utf8'));
  for (const r of rows) {
    const company = r['Company name'] || '';
    const count = Number(r['Open Roles Count'] || 0);
    let jobs = [];
    try { jobs = JSON.parse(r['Jobs JSON'] || '[]'); } catch { jobs = []; }

    const noisy = jobs.filter(isNoisyJob);
    const mismatch = count !== jobs.length;

    if (mismatch || noisy.length > 0) {
      flagged.push({
        file: f,
        company,
        openRolesCount: count,
        jobsJsonCount: jobs.length,
        mismatch,
        noisyCount: noisy.length,
        careersUrl: r['Careers URL'] || '',
        resolvedCareersUrl: r['Resolved Careers URL'] || ''
      });
    }
  }
}

fs.writeFileSync('jobs_json_quality_report.json', JSON.stringify(flagged, null, 2));
console.log(`Flagged ${flagged.length} rows -> jobs_json_quality_report.json`);
