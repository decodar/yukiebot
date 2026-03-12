import fs from 'node:fs';
const t = fs.readFileSync('bc_companies_100_people_best_effort.csv','utf8').trim().split(/\r?\n/);
const headers = t[0].split(',');
const idxName = headers.indexOf('companyName');
const idxEmp = headers.indexOf('employeeName');
const idxSrc = headers.indexOf('sourceType');
function parse(line){const out=[];let c='',q=false;for(let i=0;i<line.length;i++){const ch=line[i];if(ch==='"'){if(q&&line[i+1]==='"'){c+='"';i++;}else q=!q;}else if(ch===','&&!q){out.push(c);c='';}else c+=ch;}out.push(c);return out;}
const m = new Map();
for (const line of t.slice(1)) {
  const a = parse(line);
  const co = a[idxName] || '';
  const emp = (a[idxEmp] || '').trim();
  const src = a[idxSrc] || '';
  if (!m.has(co)) m.set(co, { n: 0, none: false });
  const o = m.get(co);
  if (src === 'none_found') o.none = true;
  if (emp) o.n++;
}
let full=0, partial=0, zero=0;
for (const v of m.values()) {
  if (v.n >= 10) full++;
  else if (v.n > 0) partial++;
  else zero++;
}
console.log(JSON.stringify({companies:m.size,full10Plus:full,partial,zero},null,2));
