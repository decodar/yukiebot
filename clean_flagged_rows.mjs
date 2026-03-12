import fs from 'node:fs';

const files = ['bc_companies_top25_enriched.csv', 'bc_companies_100_more_enriched.csv'];
const zeroOut = new Set([
  'life sciences bc',
  'bc tech',
  'unbounce',
  'klue',
  'destination british columbia',
  'isct, international society for cell & gene therapy'
]);

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

for (const file of files) {
  if (!fs.existsSync(file)) continue;
  const { headers, rows } = parseCsv(fs.readFileSync(file, 'utf8'));
  for (const row of rows) {
    const name = (row['Company name'] || '').toLowerCase();
    if (zeroOut.has(name)) {
      row['Open Roles Count'] = '0';
      row['Jobs JSON'] = '[]';
      const n = row['Notes / confidence'] || '';
      if (!n.includes('manual QA')) {
        row['Notes / confidence'] = `${n} | manual QA: non-role links removed`;
      }
    }
  }
  fs.writeFileSync(file, toCsv(headers, rows), 'utf8');
  console.log('Cleaned', file);
}
