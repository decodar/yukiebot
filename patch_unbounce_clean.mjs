import fs from 'node:fs';
const file='bc_companies_100_more_enriched.csv';

function parseCsv(content){const lines=content.split(/\r?\n/).filter(Boolean);const parse=(line)=>{const out=[];let c='',q=false;for(let i=0;i<line.length;i++){const ch=line[i];if(ch==='"'){if(q&&line[i+1]==='"'){c+='"';i++;}else q=!q;}else if(ch===','&&!q){out.push(c);c='';}else c+=ch;}out.push(c);return out;};const headers=parse(lines[0]);const rows=lines.slice(1).map(l=>{const a=parse(l);const r={};headers.forEach((h,i)=>r[h]=a[i]??'');return r;});return {headers,rows};}
function toCsv(headers,rows){const esc=(v)=>{const s=String(v??'');return /[",\n]/.test(s)?`"${s.replaceAll('"','""')}"`:s};return [headers.join(','),...rows.map(r=>headers.map(h=>esc(r[h]??'')).join(','))].join('\n');}

const {headers,rows}=parseCsv(fs.readFileSync(file,'utf8'));
for(const r of rows){if((r['Company name']||'').toLowerCase()==='unbounce'){r['Open Roles Count']='0';r['Jobs JSON']='[]';r['Notes / confidence']=(r['Notes / confidence']||'')+' | manual QA cleanup for noisy career links';}}
fs.writeFileSync(file,toCsv(headers,rows),'utf8');
console.log('patched unbounce');
