import fs from 'node:fs';

const p = 'bc_companies_100_more.csv';
const lines = fs.readFileSync(p, 'utf8').trim().split(/\r?\n/);
const header = lines[0].split(',');

function parseCsvLine(line) {
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
}

const rows = lines.slice(1).map((line) => {
  const a = parseCsvLine(line);
  const o = {};
  header.forEach((h, i) => (o[h] = a[i] || ''));

  let score = 0;
  if ((o['Open jobs count/hiring signal'] || '').toLowerCase().includes('likely')) score += 3;
  if ((o['Ownership/management signal'] || '').trim()) score += 2;
  if ((o['Telephone number'] || '').trim()) score += 1;
  if ((o['LinkedIn / Apollo URL'] || '').trim()) score += 1;
  const emp = Number(o['Employee range'] || 0);
  if (emp >= 40 && emp <= 180) score += 1;

  o._score = score;
  return o;
});

rows.sort((a, b) => b._score - a._score || a['Company name'].localeCompare(b['Company name']));
const top = rows.slice(0, 25);

const esc = (v) => /[",\n]/.test(String(v)) ? `"${String(v).replaceAll('"', '""')}"` : String(v);
const out = [header.join(',')];
for (const r of top) out.push(header.map((h) => esc(r[h] || '')).join(','));

fs.writeFileSync('bc_companies_top25.csv', out.join('\n'));
console.log('Wrote 25 rows to bc_companies_top25.csv');
