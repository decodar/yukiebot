import fs from 'node:fs';

const IN = 'bc_companies_top25.csv';
const OUT = 'bc_companies_top25_enriched.csv';

function parseCsv(content) {
  const lines = content.split(/\r?\n/).filter(Boolean);
  const rows = [];
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
  for (const line of lines.slice(1)) {
    const vals = parseLine(line);
    const row = {};
    headers.forEach((h, i) => row[h] = vals[i] ?? '');
    rows.push(row);
  }
  return { headers, rows };
}

const esc = (v) => {
  const s = String(v ?? '');
  return /[",\n]/.test(s) ? `"${s.replaceAll('"', '""')}"` : s;
};

function normSite(url) {
  if (!url) return '';
  let u = url.trim();
  if (!/^https?:\/\//i.test(u)) u = `https://${u}`;
  try {
    const x = new URL(u);
    return `${x.protocol}//${x.host}`;
  } catch { return ''; }
}

async function fetchText(url) {
  try {
    const res = await fetch(url, { redirect: 'follow' });
    const text = await res.text();
    return { ok: res.ok, url: res.url, text };
  } catch {
    return { ok: false, url, text: '' };
  }
}

function extractGreenhouseSlug(html) {
  const m = html.match(/greenhouse\.io\/embed\/job_board\?for=([a-z0-9_-]+)/i)
    || html.match(/boards\.greenhouse\.io\/([a-z0-9_-]+)/i);
  return m ? m[1] : '';
}

function extractLeverSlug(html) {
  const m = html.match(/jobs\.lever\.co\/([a-z0-9_-]+)/i)
    || html.match(/api\.lever\.co\/v0\/postings\/([a-z0-9_-]+)/i);
  return m ? m[1] : '';
}

async function getJobsFromGreenhouse(slug) {
  if (!slug) return [];
  try {
    const r = await fetch(`https://boards-api.greenhouse.io/v1/boards/${slug}/jobs`);
    if (!r.ok) return [];
    const j = await r.json();
    return (j.jobs || []).map(x => ({ jobID: String(x.id), jobName: x.title || '' }));
  } catch { return []; }
}

async function getJobsFromLever(slug) {
  if (!slug) return [];
  try {
    const r = await fetch(`https://api.lever.co/v0/postings/${slug}?mode=json`);
    if (!r.ok) return [];
    const j = await r.json();
    return (j || []).map(x => ({ jobID: x.id || '', jobName: x.text || '' }));
  } catch { return []; }
}

function genericJobParse(html) {
  const jobs = [];
  const re = /<a[^>]+href=["']([^"']+)["'][^>]*>(.*?)<\/a>/gis;
  let m;
  while ((m = re.exec(html)) !== null) {
    const href = m[1] || '';
    const text = (m[2] || '').replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();
    if (!text) continue;
    const h = href.toLowerCase();
    const t = text.toLowerCase();
    const likely = /job|career|position|opening|engineer|manager|developer|analyst|specialist|associate/.test(t)
      && !/learn more|read more|about|privacy|cookie|terms|contact/.test(t)
      && !/\/careers?$|\/jobs?$/.test(h);
    if (likely) jobs.push({ jobID: href, jobName: text });
  }
  // de-dup by name
  const seen = new Set();
  return jobs.filter(j => {
    const k = j.jobName.toLowerCase();
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  }).slice(0, 50);
}

async function enrichRow(row) {
  const base = normSite(row['Website']);
  const candidates = [
    `${base}/careers`, `${base}/career`, `${base}/jobs`, `${base}/join-us`, `${base}/company/careers`
  ].filter(u => u && !u.includes('://undefined'));

  let careerUrl = '';
  let html = '';
  for (const u of candidates) {
    const r = await fetchText(u);
    if (r.ok && r.text && r.text.length > 500) {
      careerUrl = r.url || u;
      html = r.text;
      break;
    }
  }

  if (!careerUrl) {
    row['Careers URL'] = '';
    row['Open Roles Count'] = '';
    row['Jobs JSON'] = '[]';
    row['Notes / confidence'] = `${row['Notes / confidence']} | careers page not auto-found`;
    return row;
  }

  let jobs = [];
  const gh = extractGreenhouseSlug(html);
  const lv = extractLeverSlug(html);
  if (gh) jobs = await getJobsFromGreenhouse(gh);
  if (!jobs.length && lv) jobs = await getJobsFromLever(lv);
  if (!jobs.length) jobs = genericJobParse(html);

  row['Careers URL'] = careerUrl;
  row['Open Roles Count'] = jobs.length ? String(jobs.length) : '0';
  row['Jobs JSON'] = JSON.stringify(jobs);
  return row;
}

async function run() {
  const { headers, rows } = parseCsv(fs.readFileSync(IN, 'utf8'));
  const extra = ['Careers URL', 'Open Roles Count', 'Jobs JSON'];

  for (let i = 0; i < rows.length; i++) {
    rows[i] = await enrichRow(rows[i]);
    await new Promise(r => setTimeout(r, 250));
  }

  const outHeaders = [...headers, ...extra.filter(h => !headers.includes(h))];
  const lines = [outHeaders.join(',')];
  for (const row of rows) {
    lines.push(outHeaders.map(h => esc(row[h] ?? '')).join(','));
  }

  fs.writeFileSync(OUT, lines.join('\n'), 'utf8');
  console.log(`Wrote ${rows.length} rows to ${OUT}`);
}

run().catch(err => {
  console.error(err);
  process.exit(1);
});
