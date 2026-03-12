import fs from 'node:fs';

const IN = 'bc_companies_100_more_enriched.csv';
const OUT = 'bc_companies_100_people_best_effort.csv';

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
        } else q = !q;
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
  return rows;
}

const esc = (v) => {
  const s = String(v ?? '');
  return /[",\n]/.test(s) ? `"${s.replaceAll('"', '""')}"` : s;
};

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function baseSite(url) {
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
    const r = await fetch(url, { redirect: 'follow' });
    const t = await r.text();
    return { ok: r.ok, url: r.url, text: t };
  } catch {
    return { ok: false, url, text: '' };
  }
}

function clean(s) {
  return String(s || '').replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();
}

function parseJsonLdPeople(html) {
  const out = [];
  const scripts = [...html.matchAll(/<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi)].map(m => m[1]);
  for (const raw of scripts) {
    try {
      const data = JSON.parse(raw);
      const items = Array.isArray(data) ? data : [data];
      for (const it of items) {
        const queue = [it];
        while (queue.length) {
          const n = queue.shift();
          if (!n || typeof n !== 'object') continue;
          if ((n['@type'] || '').toString().toLowerCase() === 'person') {
            out.push({
              name: n.name || '',
              title: n.jobTitle || '',
              profileUrl: n.url || '',
              sourceType: 'jsonld'
            });
          }
          for (const v of Object.values(n)) {
            if (Array.isArray(v)) queue.push(...v);
            else if (v && typeof v === 'object') queue.push(v);
          }
        }
      }
    } catch {}
  }
  return out;
}

function parseLinkedinProfileHints(html) {
  const out = [];
  const re = /https?:\/\/([a-z]{2,3}\.)?linkedin\.com\/in\/[^"'\s<)]+/gi;
  const links = [...html.matchAll(re)].map(m => m[0]);
  for (const l of [...new Set(links)]) {
    const slug = l.split('/in/')[1]?.split(/[/?#]/)[0] || '';
    const nameGuess = slug.replace(/[-_]/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
    out.push({ name: nameGuess, title: '', profileUrl: l, sourceType: 'linkedin_hint' });
  }
  return out;
}

function parseNameTitlePairs(html) {
  const out = [];
  const blocks = [...html.matchAll(/<(h1|h2|h3|h4|strong)[^>]*>([\s\S]{1,120}?)<\/\1>/gi)];
  for (const m of blocks) {
    const name = clean(m[2]);
    if (!/^[A-Z][A-Za-z'\-]+(\s+[A-Z][A-Za-z'\-\.]+){1,3}$/.test(name)) continue;
    const idx = m.index || 0;
    const context = html.slice(Math.max(0, idx), Math.min(html.length, idx + 500));
    const titleMatch = context.match(/(CEO|Chief [A-Za-z ]+|Founder|Co-?Founder|President|Director|Manager|VP|Vice President|Head of [A-Za-z ]+|Principal|Partner|Lead [A-Za-z ]+)/i);
    out.push({ name, title: titleMatch ? titleMatch[1] : '', profileUrl: '', sourceType: 'name_block' });
  }
  return out;
}

function dedupePeople(list) {
  const seen = new Set();
  const out = [];
  for (const p of list) {
    const k = `${(p.name||'').toLowerCase()}|${(p.profileUrl||'').toLowerCase()}`;
    if (!p.name || p.name.length < 4) continue;
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(p);
  }
  return out;
}

async function getPeopleForCompany(website, linkedinCompanyUrl) {
  const base = baseSite(website);
  if (!base) return { people: [], sources: [] };

  const paths = ['/team', '/about', '/about-us', '/leadership', '/company', '/our-team', '/who-we-are'];
  const urls = [base, ...paths.map(p => base + p)];

  let all = [];
  const visited = [];
  for (const u of urls) {
    const r = await fetchText(u);
    if (!r.ok || !r.text) continue;
    visited.push(r.url || u);
    const html = r.text;
    all.push(...parseJsonLdPeople(html));
    all.push(...parseNameTitlePairs(html));
    all.push(...parseLinkedinProfileHints(html));
    await sleep(80);
    if (dedupePeople(all).length >= 12) break;
  }

  const people = dedupePeople(all).slice(0, 10).map((p, i) => ({
    rank: i + 1,
    name: p.name,
    title: p.title,
    profileUrl: p.profileUrl,
    sourceType: p.sourceType,
    sourceUrl: visited[0] || base,
    companyLinkedIn: linkedinCompanyUrl || ''
  }));

  return { people, sources: visited };
}

async function run() {
  const companies = parseCsv(fs.readFileSync(IN, 'utf8'));
  const rows = [];
  let done = 0;

  for (const c of companies) {
    const companyName = c['Company name'] || '';
    const website = c['Website'] || '';
    const linkedin = c['LinkedIn / Apollo URL'] || '';

    const { people } = await getPeopleForCompany(website, linkedin);

    if (!people.length) {
      rows.push({
        companyName,
        website,
        employeeRank: '',
        employeeName: '',
        employeeTitle: '',
        seniority: '',
        function: '',
        profileUrl: '',
        companyLinkedIn: linkedin,
        sourceType: 'none_found',
        sourceUrl: website,
        confidence: 'low'
      });
    } else {
      for (const p of people) {
        const titleLower = (p.title || '').toLowerCase();
        const seniority = /chief|ceo|cto|cfo|coo|president|founder/.test(titleLower) ? 'C-level/Founder'
          : /vp|vice president|director|head/.test(titleLower) ? 'Director/VP'
          : /manager|lead/.test(titleLower) ? 'Manager/Lead'
          : p.title ? 'IC/Other' : '';
        const func = /engineer|developer|technical|cto|technology/.test(titleLower) ? 'Engineering'
          : /sales|revenue|account/.test(titleLower) ? 'Sales'
          : /marketing|growth/.test(titleLower) ? 'Marketing'
          : /people|hr|talent/.test(titleLower) ? 'People/HR'
          : /product/.test(titleLower) ? 'Product'
          : /finance|cfo/.test(titleLower) ? 'Finance'
          : '';

        rows.push({
          companyName,
          website,
          employeeRank: p.rank,
          employeeName: p.name,
          employeeTitle: p.title,
          seniority,
          function: func,
          profileUrl: p.profileUrl,
          companyLinkedIn: linkedin,
          sourceType: p.sourceType,
          sourceUrl: p.sourceUrl,
          confidence: p.sourceType === 'jsonld' ? 'high' : (p.sourceType === 'name_block' ? 'medium' : 'low')
        });
      }
    }

    done++;
    if (done % 10 === 0) console.log(`Processed ${done}/${companies.length}`);
    await sleep(120);
  }

  const headers = [
    'companyName','website','employeeRank','employeeName','employeeTitle','seniority','function',
    'profileUrl','companyLinkedIn','sourceType','sourceUrl','confidence'
  ];
  const out = [headers.join(',')];
  for (const r of rows) out.push(headers.map(h => esc(r[h] ?? '')).join(','));
  fs.writeFileSync(OUT, out.join('\n'), 'utf8');
  console.log(`Wrote ${rows.length} rows to ${OUT}`);
}

run().catch(err => { console.error(err); process.exit(1); });
