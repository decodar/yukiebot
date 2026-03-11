import os
import csv
import time
import requests

API_KEY = os.getenv("APOLLO_API_KEY")
if not API_KEY:
    raise SystemExit("Missing APOLLO_API_KEY env var")

BASE = "https://api.apollo.io/api/v1"
HEADERS = {"Content-Type": "application/json", "X-Api-Key": API_KEY}

# ---- helpers ----
def post(path, payload):
    url = f"{BASE}{path}"
    r = requests.post(url, json=payload, headers=HEADERS, timeout=45)
    if r.status_code >= 400:
        raise RuntimeError(f"{r.status_code} {url}\n{r.text[:1000]}")
    return r.json()


def get(path, params=None):
    url = f"{BASE}{path}"
    r = requests.get(url, params=params or {}, headers=HEADERS, timeout=45)
    if r.status_code >= 400:
        raise RuntimeError(f"{r.status_code} {url}\n{r.text[:1000]}")
    return r.json()


def owner_signal(org):
    txt = " ".join([
        str(org.get("short_description", "")),
        str(org.get("keywords", "")),
        str(org.get("name", "")),
    ]).lower()
    signals = ["founder-led", "founder", "owner", "privately held", "family-owned", "entrepreneur"]
    hit = [s for s in signals if s in txt]
    return ", ".join(hit) if hit else ""


def first_nonempty(*vals):
    for v in vals:
        if v:
            return v
    return ""


# ---- step 1: search companies in BC, 25-250 employees ----
# NOTE: Apollo field names can vary by plan/version.
# If your account errors, print the error and adjust these filter keys accordingly.
company_payload = {
    "page": 1,
    "per_page": 50,
    "organization_num_employees_ranges": ["25,50", "51,100", "101,200", "201,500"],
    "organization_locations": ["British Columbia, Canada"],
    "sort_by_field": "organization_num_employees",
    "sort_ascending": True
}

search = post("/mixed_companies/search", company_payload)
orgs = search.get("organizations", []) or search.get("accounts", []) or []

# ---- step 2: keep only those with at least 1 open job (hiring signal) ----
# Apollo doesn't always return exact open job count in company search response.
# We'll use available hiring fields/signals when present.
filtered = []
for o in orgs:
    hiring_signal = any([
        o.get("is_hiring") is True,
        (o.get("job_postings_count") or 0) > 0,
        (o.get("active_job_count") or 0) > 0
    ])
    if hiring_signal:
        filtered.append(o)

# fallback: if API response lacks explicit hiring fields, keep top orgs and enrich via people/job signals
if len(filtered) < 10:
    filtered = orgs[:30]

# ---- step 3: for each org, find CEO/manager contact ----
rows = []
seen = set()

for o in filtered:
    org_id = o.get("id") or o.get("organization_id")
    if not org_id or org_id in seen:
        continue
    seen.add(org_id)

    org_name = o.get("name", "")
    website = first_nonempty(o.get("website_url"), o.get("primary_domain"))
    hq_city = first_nonempty(o.get("city"), o.get("organization_city"))
    hq_state = first_nonempty(o.get("state"), o.get("organization_state"))
    hq = f"{hq_city}, {hq_state}".strip(", ")
    employees = first_nonempty(o.get("estimated_num_employees"), o.get("organization_num_employees"))
    open_jobs = first_nonempty(o.get("job_postings_count"), o.get("active_job_count"), o.get("is_hiring"))
    mgmt_signal = owner_signal(o)
    linkedin = first_nonempty(o.get("linkedin_url"), o.get("organization_linkedin_url"))
    apollo_url = first_nonempty(o.get("apollo_url"), o.get("organization_apollo_url"))

    # People search: prioritize CEO/founder/owner/manager titles at this org
    people_payload = {
        "page": 1,
        "per_page": 5,
        "organization_ids": [org_id],
        "person_titles": ["CEO", "Chief Executive Officer", "Founder", "Owner", "General Manager", "Managing Director", "President"]
    }

    person_name = person_title = person_email = person_phone = ""
    try:
        p = post("/mixed_people/search", people_payload)
        people = p.get("people", []) or p.get("contacts", [])
        if people:
            person = people[0]
            person_name = first_nonempty(person.get("name"), f"{person.get('first_name', '')} {person.get('last_name', '')}".strip())
            person_title = person.get("title", "")
            person_email = first_nonempty(person.get("email"), person.get("work_email"))
            person_phone = first_nonempty(person.get("phone"), person.get("phone_number"), person.get("mobile_phone"))
    except Exception:
        pass

    rows.append({
        "Company name": org_name,
        "Website": website,
        "HQ city (BC)": hq,
        "Employee range": employees,
        "Ownership/management signal": mgmt_signal,
        "Open jobs count/hiring signal": open_jobs,
        "CEO or manager name": person_name,
        "CEO/manager title": person_title,
        "Email address": person_email,
        "Telephone number": person_phone,
        "LinkedIn / Apollo URL": linkedin or apollo_url,
        "Notes / confidence": "Medium (API field availability varies by org/profile)"
    })

    if len(rows) >= 10:
        break
    time.sleep(0.2)

# ---- write CSV ----
out = "bc_companies_10.csv"
fields = [
    "Company name", "Website", "HQ city (BC)", "Employee range",
    "Ownership/management signal", "Open jobs count/hiring signal",
    "CEO or manager name", "CEO/manager title", "Email address", "Telephone number",
    "LinkedIn / Apollo URL", "Notes / confidence"
]
with open(out, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=fields)
    w.writeheader()
    w.writerows(rows)

print(f"Wrote {len(rows)} rows to {out}")
