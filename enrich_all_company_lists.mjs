import fs from 'node:fs';

const FILES = [
  { in: 'bc_companies_top25.csv', out: 'bc_companies_top25_enriched.csv' },
  { in: 'bc_companies_100_more.csv', out: 'bc_companies_100_more_enriched.csv' },
];

function parseCsv(content) {
  const lines = content.split(/\r?\n/).filter(Boolean);
  const parseLine = (line) => {
    const out = [];
    let cur = '';
    let q = false;
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (ch === '"') {
        if (q && line[i + 1] === '"') {
          cur += '"';
          i++;
        } else {
          q = !q;
        }
      } else if (ch === ',' && !q) {
        out.push(cur);
        cur = '';
      } else {
        cur += ch;
      }
    }
    out.push(cur);
    return out;
  };

  const headers = parseLine(lines[0]);
  const rows = lines.slice(1).map((line) => {
    const vals = parseLine(line);
    const row = {};
    headers.forEach((h, i) => (row[h] = vals[i] ?? ''));
    return row;
  });

  return { headers, rows };
}

function toCsv(headers, rows) {
  const esc = (v) => {
    const s = String(v ?? '');
    return /[",\n]/.test(s) ? `"${s.replaceAll('"', '""')}"` : s;
  };
  const lines = [headers.join(',')];
  for (const row of rows) lines.push(headers.map((h) => esc(row[h] ?? '')).join(','));
  return lines.join('\n');
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normSite(url) {
  if (!url) return '';
  let u = url.trim();
  if (!/^https?:\/\//i.test(u)) u = `https://${u}`;
  try {
    const x = new URL(u);
    return `${x.protocol}//${x.host}`;
  } catch {
    return '';
  }
}

function absUrl(base, maybeRelative) {
  try {
    return new URL(maybeRelative, base).toString();
  } catch {
    return maybeRelative || '';
  }
}

async function fetchPage(url) {
  try {
    const res = await fetch(url, { redirect: 'follow' });
    const text = await res.text();
    return { ok: res.ok, status: res.status, finalUrl: res.url, text };
  } catch {
    return { ok: false, status: 0, finalUrl: url, text: '' };
  }
}

function extractGreenhouseSlug(html) {
  return (
    html.match(/job-boards\.greenhouse\.io\/embed\/job_board\?for=([a-z0-9_-]+)/i)?.[1] ||
    html.match(/boards\.greenhouse\.io\/([a-z0-9_-]+)/i)?.[1] ||
    ''
  );
}

function extractLeverSlug(html) {
  return (
    html.match(/jobs\.lever\.co\/([a-z0-9_-]+)/i)?.[1] ||
    html.match(/api\.lever\.co\/v0\/postings\/([a-z0-9_-]+)/i)?.[1] ||
    ''
  );
}

function extractFirstRoleLink(html, baseUrl) {
  const linkRegex = /<a[^>]+href=["']([^"']+)["'][^>]*>(.*?)<\/a>/gis;
  let m;
  while ((m = linkRegex.exec(html)) !== null) {
    const href = m[1] || '';
    const txt = (m[2] || '').replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim().toLowerCase();
    if (!txt) continue;
    if (/(i am interested|show me open roles|see open roles|view open roles|open positions|current openings|view jobs|see jobs|careers)/i.test(txt)) {
      return absUrl(baseUrl, href);
    }
  }
  return '';
}

async function jobsFromGreenhouse(slug) {
  if (!slug) return [];
  try {
    const r = await fetch(`https://boards-api.greenhouse.io/v1/boards/${slug}/jobs`);
    if (!r.ok) return [];
    const j = await r.json();
    return (j.jobs || []).map((x) => ({
      jobID: String(x.id || ''),
      jobName: x.title || '',
      department: x.departments?.[0]?.name || '',
      location: x.location?.name || '',
      employmentType: '',
      postedAt: x.updated_at || '',
      applyUrl: x.absolute_url || '',
    }));
  } catch {
    return [];
  }
}

async function jobsFromLever(slug) {
  if (!slug) return [];
  try {
    const r = await fetch(`https://api.lever.co/v0/postings/${slug}?mode=json`);
    if (!r.ok) return [];
    const j = await r.json();
    return (j || []).map((x) => ({
      jobID: x.id || '',
      jobName: x.text || '',
      department: x.categories?.team || '',
      location: x.categories?.location || '',
      employmentType: x.categories?.commitment || '',
      postedAt: x.createdAt ? new Date(x.createdAt).toISOString() : '',
      applyUrl: x.hostedUrl || x.applyUrl || '',
    }));
  } catch {
    return [];
  }
}

function jobsFromAdpLike(html, baseUrl) {
  const out = [];

  const countMatch = html.match(/Current Openings\s*\((\d+)\s*of\s*(\d+)\)/i);
  const countHint = countMatch ? Number(countMatch[2]) : null;

  const linkRegex = /<a[^>]*>(.*?)<\/a>/gis;
  let m;
  while ((m = linkRegex.exec(html)) !== null) {
    const text = (m[1] || '').replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();
    if (!text) continue;
    if (/search|sign in|career centre|current openings|join our talent community|privacy|legal|requirements/i.test(text)) continue;
    if (/manager|director|engineer|analyst|specialist|coordinator|developer|operations|sales|product|scientist|associate/i.test(text)) {
      out.push({
        jobID: text,
        jobName: text,
        department: '',
        location: '',
        employmentType: '',
        postedAt: '',
        applyUrl: baseUrl,
      });
    }
  }

  const dedup = [];
  const seen = new Set();
  for (const j of out) {
    const k = j.jobName.toLowerCase();
    if (seen.has(k)) continue;
    seen.add(k);
    dedup.push(j);
  }

  if (countHint && dedup.length > countHint) return dedup.slice(0, countHint);
  return dedup;
}

function jobsFromGenericLinks(html, baseUrl) {
  const out = [];
  const linkRegex = /<a[^>]+href=["']([^"']+)["'][^>]*>(.*?)<\/a>/gis;
  let m;
  while ((m = linkRegex.exec(html)) !== null) {
    const href = m[1] || '';
    const text = (m[2] || '').replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();
    const t = text.toLowerCase();
    if (!text) continue;
    if (/job board|post a job|job seekers|careers|learn more|read more|contact|about|privacy|cookie|terms|home/i.test(t)) continue;
    if (/manager|director|engineer|analyst|specialist|coordinator|developer|operations|sales|product|scientist|associate|officer|administrator|designer/i.test(t)) {
      out.push({
        jobID: href,
        jobName: text,
        department: '',
        location: '',
        employmentType: '',
        postedAt: '',
        applyUrl: absUrl(baseUrl, href),
      });
    }
  }

  const dedup = [];
  const seen = new Set();
  for (const j of out) {
    const k = `${j.jobName.toLowerCase()}|${j.applyUrl}`;
    if (seen.has(k)) continue;
    seen.add(k);
    dedup.push(j);
  }
  return dedup.slice(0, 100);
}

async function enrichRow(row) {
  const website = row['Website'] || '';
  const base = normSite(website);

  const ensureColumns = {
    'Careers URL': row['Careers URL'] || '',
    'Resolved Careers URL': row['Resolved Careers URL'] || '',
    'Open Roles Count': row['Open Roles Count'] || '',
    'Jobs JSON': row['Jobs JSON'] || '[]',
  };
  Object.assign(row, ensureColumns);

  if (!base) {
    row['Careers URL'] = '';
    row['Resolved Careers URL'] = '';
    row['Open Roles Count'] = '';
    row['Jobs JSON'] = '[]';
    return row;
  }

  const candidates = [
    `${base}/careers`,
    `${base}/career`,
    `${base}/jobs`,
    `${base}/join-us`,
    `${base}/company/careers`,
  ];

  let seedPage = null;
  let seedUrl = '';
  for (const c of candidates) {
    const p = await fetchPage(c);
    if (p.ok && p.text && p.text.length > 300) {
      seedPage = p;
      seedUrl = c;
      break;
    }
    await sleep(100);
  }

  if (!seedPage) {
    row['Careers URL'] = '';
    row['Resolved Careers URL'] = '';
    row['Open Roles Count'] = '';
    row['Jobs JSON'] = '[]';
    return row;
  }

  row['Careers URL'] = seedUrl;
  let resolvedUrl = seedPage.finalUrl || seedUrl;
  let html = seedPage.text;

  const nextLink = extractFirstRoleLink(html, resolvedUrl);
  if (nextLink && !nextLink.includes('mailto:')) {
    const nextPage = await fetchPage(nextLink);
    if (nextPage.ok && nextPage.text && nextPage.text.length > 200) {
      resolvedUrl = nextPage.finalUrl || nextLink;
      html = nextPage.text;
    }
  }

  row['Resolved Careers URL'] = resolvedUrl;

  let jobs = [];
  const ghSlug = extractGreenhouseSlug(html);
  const lvSlug = extractLeverSlug(html);

  if (ghSlug) jobs = await jobsFromGreenhouse(ghSlug);
  if (!jobs.length && lvSlug) jobs = await jobsFromLever(lvSlug);

  if (!jobs.length && /workforcenow\.adp\.com|Current Openings \(/i.test(html)) {
    jobs = jobsFromAdpLike(html, resolvedUrl);
  }

  if (!jobs.length) jobs = jobsFromGenericLinks(html, resolvedUrl);

  row['Open Roles Count'] = String(jobs.length);
  row['Jobs JSON'] = JSON.stringify(jobs);

  return row;
}

async function processFile(inputFile, outputFile) {
  if (!fs.existsSync(inputFile)) {
    console.log(`Skip missing file: ${inputFile}`);
    return;
  }

  const { headers, rows } = parseCsv(fs.readFileSync(inputFile, 'utf8'));

  const wanted = ['Careers URL', 'Resolved Careers URL', 'Open Roles Count', 'Jobs JSON'];
  for (const col of wanted) {
    if (!headers.includes(col)) headers.push(col);
  }

  for (let i = 0; i < rows.length; i++) {
    rows[i] = await enrichRow(rows[i]);
    if ((i + 1) % 10 === 0) console.log(`${inputFile}: ${i + 1}/${rows.length}`);
    await sleep(150);
  }

  fs.writeFileSync(outputFile, toCsv(headers, rows), 'utf8');
  console.log(`Wrote ${rows.length} rows -> ${outputFile}`);
}

(async () => {
  for (const f of FILES) {
    await processFile(f.in, f.out);
  }
})();
