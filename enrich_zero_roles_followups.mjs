import fs from 'node:fs';

const files = ['bc_companies_top25_enriched.csv', 'bc_companies_100_more_enriched.csv'];

const FOLLOW_PATTERNS = [
  /current opportunities/i,
  /open roles?/i,
  /open positions?/i,
  /current positions?/i,
  /view (open )?roles?/i,
  /view (all )?jobs?/i,
  /see (open )?roles?/i,
  /search jobs?/i,
  /career opportunities/i,
  /join (our )?team/i,
  /work with us/i,
  /i am interested/i,
  /show me open roles/i,
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

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function fetchPage(url) {
  try {
    const res = await fetch(url, { redirect: 'follow' });
    const text = await res.text();
    return { ok: res.ok, status: res.status, finalUrl: res.url, text };
  } catch {
    return { ok: false, status: 0, finalUrl: url, text: '' };
  }
}

function absUrl(base, maybeRelative) {
  try { return new URL(maybeRelative, base).toString(); } catch { return ''; }
}

function extractLinks(html, baseUrl) {
  const links = [];
  const re = /<a[^>]+href=["']([^"']+)["'][^>]*>(.*?)<\/a>/gis;
  let m;
  while ((m = re.exec(html)) !== null) {
    const href = m[1] || '';
    const txt = (m[2] || '').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
    const abs = absUrl(baseUrl, href);
    if (!abs || abs.startsWith('mailto:') || abs.startsWith('javascript:')) continue;
    links.push({ text: txt, href: abs });
  }
  return links;
}

function findFollowTargets(html, baseUrl) {
  const links = extractLinks(html, baseUrl);
  const picked = [];
  for (const l of links) {
    if (!l.text) continue;
    if (FOLLOW_PATTERNS.some((p) => p.test(l.text))) picked.push(l.href);
  }
  // add direct ATS links if present
  const ats = [
    ...html.matchAll(/https?:\/\/[^"'\s>]*greenhouse[^"'\s>]*/gi),
    ...html.matchAll(/https?:\/\/[^"'\s>]*lever\.co[^"'\s>]*/gi),
    ...html.matchAll(/https?:\/\/[^"'\s>]*workforcenow\.adp\.com[^"'\s>]*/gi),
    ...html.matchAll(/https?:\/\/[^"'\s>]*myworkdayjobs\.com[^"'\s>]*/gi),
  ].map(m => m[0]);
  for (const u of ats) picked.push(u.replace(/&amp;/g, '&'));

  return [...new Set(picked)].slice(0, 8);
}

function extractGreenhouseSlug(html) {
  return (
    html.match(/job-boards\.greenhouse\.io\/embed\/job_board\?for=([a-z0-9_-]+)/i)?.[1] ||
    html.match(/boards\.greenhouse\.io\/([a-z0-9_-]+)/i)?.[1] ||
    html.match(/greenhouse\.io\/([^/?"']+)/i)?.[1] ||
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
  } catch { return []; }
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
  } catch { return []; }
}

function jobsFromAdp(html, url) {
  const jobs = [];
  const cardRe = /<a[^>]*>(.*?)<\/a>/gis;
  let m;
  while ((m = cardRe.exec(html)) !== null) {
    const t = (m[1] || '').replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();
    if (!t) continue;
    if (/sign in|career centre|current openings|join our talent community|privacy|legal|requirements|search/i.test(t)) continue;
    if (/manager|director|engineer|analyst|specialist|coordinator|developer|operations|sales|product|scientist|associate/i.test(t)) {
      jobs.push({ jobID: t, jobName: t, department: '', location: '', employmentType: '', postedAt: '', applyUrl: url });
    }
  }
  const seen = new Set();
  return jobs.filter(j => { const k = j.jobName.toLowerCase(); if (seen.has(k)) return false; seen.add(k); return true; }).slice(0, 25);
}

function jobsFromGeneric(html, baseUrl) {
  const jobs = [];
  for (const l of extractLinks(html, baseUrl)) {
    const t = (l.text || '').toLowerCase();
    if (!t) continue;
    if (/about|privacy|cookie|terms|board of directors|member directory|contact|home|product|pricing/.test(t)) continue;
    if (/manager|director|engineer|analyst|specialist|coordinator|developer|operations|sales|product manager|scientist|associate|officer|administrator|designer|intern/i.test(l.text)) {
      jobs.push({ jobID: l.href, jobName: l.text, department: '', location: '', employmentType: '', postedAt: '', applyUrl: l.href });
    }
  }
  const seen = new Set();
  return jobs.filter(j => { const k = (j.jobName + '|' + j.applyUrl).toLowerCase(); if (seen.has(k)) return false; seen.add(k); return true; }).slice(0, 50);
}

async function parseJobsFromPage(url, html) {
  // ATS first
  let jobs = [];
  const gh = extractGreenhouseSlug(html);
  const lv = extractLeverSlug(html);
  if (gh) jobs = await jobsFromGreenhouse(gh);
  if (!jobs.length && lv) jobs = await jobsFromLever(lv);
  if (!jobs.length && /workforcenow\.adp\.com|Current Openings \(/i.test(html)) jobs = jobsFromAdp(html, url);
  if (!jobs.length) jobs = jobsFromGeneric(html, url);
  return jobs;
}

async function enrichZeroRow(row) {
  const company = row['Company name'] || '';
  const start = row['Resolved Careers URL'] || row['Careers URL'];
  if (!start) return false;

  const p1 = await fetchPage(start);
  if (!p1.ok || !p1.text) return false;

  let jobs = await parseJobsFromPage(p1.finalUrl, p1.text);
  let resolved = p1.finalUrl;

  if (!jobs.length) {
    const targets = findFollowTargets(p1.text, p1.finalUrl);
    for (const t of targets) {
      const p2 = await fetchPage(t);
      if (!p2.ok || !p2.text) continue;
      const j2 = await parseJobsFromPage(p2.finalUrl, p2.text);
      if (j2.length) {
        jobs = j2;
        resolved = p2.finalUrl;
        break;
      }
      // one more depth
      const targets2 = findFollowTargets(p2.text, p2.finalUrl);
      for (const t2 of targets2) {
        const p3 = await fetchPage(t2);
        if (!p3.ok || !p3.text) continue;
        const j3 = await parseJobsFromPage(p3.finalUrl, p3.text);
        if (j3.length) {
          jobs = j3;
          resolved = p3.finalUrl;
          break;
        }
      }
      if (jobs.length) break;
      await sleep(120);
    }
  }

  if (!jobs.length) return false;

  row['Resolved Careers URL'] = resolved;
  row['Open Roles Count'] = String(jobs.length);
  row['Jobs JSON'] = JSON.stringify(jobs);
  row['Notes / confidence'] = `${row['Notes / confidence'] || ''} | follow-link enrichment applied`;
  console.log(`Enriched ${company}: ${jobs.length}`);
  return true;
}

for (const file of files) {
  if (!fs.existsSync(file)) continue;
  const { headers, rows } = parseCsv(fs.readFileSync(file, 'utf8'));
  let touched = 0;
  for (let i = 0; i < rows.length; i++) {
    const c = Number(rows[i]['Open Roles Count'] || 0);
    if (c === 0) {
      const ok = await enrichZeroRow(rows[i]);
      if (ok) touched++;
      await sleep(150);
    }
  }
  fs.writeFileSync(file, toCsv(headers, rows), 'utf8');
  console.log(`Updated ${file}; enriched ${touched} zero-role rows`);
}
