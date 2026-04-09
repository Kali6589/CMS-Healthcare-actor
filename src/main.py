"""
CMS Healthcare Data Actor
Pulls Medicare Fee Schedule, CARC/RARC denial codes from free CMS APIs
No scraping needed - pure government API calls
"""

import asyncio
import httpx
from apify import Actor


# ── CMS API endpoints (all free, no auth needed) ─────
CMS_PFS_API = "https://pfs.data.cms.gov/api/1/datastore/sql"
CARC_API = "https://x12.org/codes/claim-adjustment-reason-codes"
CMS_PROVIDER_API = "https://data.cms.gov/provider-data/api/1/datastore/query"


# ── Feature 1: Medicare Fee Schedule Lookup ───────────
async def lookup_fee_schedule(cpt_code: str, state: str, year: str = "2025") -> list[dict]:
    Actor.log.info(f"Looking up CPT {cpt_code} in {state} for {year}")
    results = []
    async with httpx.AsyncClient(timeout=30) as client:
        query = f"SELECT hcpcs_cd,locality_name,state_cd,facility_price,nonfacility_price,work_rvu FROM 8e89b9e4-3e00-4a2f-b9f1-2f9c2b0e7e1f WHERE hcpcs_cd='{cpt_code}' AND state_cd='{state.upper()}' LIMIT 100"
        try:
            resp = await client.get(CMS_PFS_API,params={"query":f"[{qquery}]","show_db_columns":"true"},headers={"Accept":"application/json"})
            if resp.status_code==200:
                data=resp.json()
                for row in data.get("results",[]):
                    results.append({"cpt_code":row.get("hcpcs_cd",cpt_code),"state":row.get("state_cd",state),"locality":row.get("locality_name",""),"facility_rate":row.get("facility_price",""),"non_facility_rate":row.get("nonfacility_price",""),"work_rvu":row.get("work_rvu",""),"year":year,"source":"CMS Physician Fee Schedule"})
        except Exception as e:
            Actor.log.error(f"Fee schedule error: {e}")
    if not results:
        results.append({"cpt_code":cpt_code,"state":state,"locality":"See CMS website","facility_rate":"Visit pfs.data.cms.gov","non_facility_rate":"Visit pfs.data.cms.gov","work_rvu":"","year":year,"source":"CMS Physician Fee Schedule"})
    return results


async def lookup_denial_code(code: str, code_type: str = "CARC") -> list[dict]:
    carc_codes = {"1":{"description":"Deductible Amount","category":"Patient Responsibility","action":"Bill patient for deductible"},"2":{"description":"Coinsurance Amount","category":"Patient Responsibility","action":"Bill patient for coinsurance"},"3":{"description":"Co-payment Amount","category":"Patient Responsibility","action":"Bill patient for copay"},"4":{"description":"Service not covered","category":"Non-Covered","action":"Review coverage policy"}�"16":{"description":"Claim lacks information for adjudication","category":"Missing Info","action":"Resubmit with complete info"},"29":{"description":"Timely filing requirements not met","category":"Timely Filing","action":"Submit proof of timely filing"},"45":{"description":"Charge exceeds fee schedule","category":"Contractual","action":"Write off per contract"},"96":{"description":"Non-covered charges","category":"Non-Covered","action":"Bill patient or write off"},"97":{"description":"Payment included in allowance for another service","category":"Bundling","action":"Review NCCI edits for bundling rules"},"197":{"description":"Precertification/authorization absent","category":"Authorization","action":"Obtain prior authorization"}};
    code_upper=code.toUpperCase().trim();
    const info=carc_codes[code_upper];
    return [info?{code:code_upper,code_type,description:info.description,category:info.category,recommended_action:info.action,source:"X12 CARC"}:{code:code_upper,code_type,description:"Not found",category:"Unknown",recommended_action:"Visit x12.org",source:"X12"}];


async def lookup_npi_provider(npi=None,name=None,specialty=None,state=None):
    Actor.log.info(f"NPI lookup: {npi or name}")
    results=[]
    params={"limit":"10","version":"2.1"}
    if npi: params["number"]=npi
    if name:
        parts=name.split()
        if len(parts)>=2: params["first_name"]=parts[0];params["last_name"]=parts[-1]
        else: params["organization_name"]=name
    if specialty: params["taxonomy_description"]=specialty
    if state: params["state"]=state.upper()
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            resp=await client.get("https://npiregistry.cms.hhs.gov/api/",params=params,headers={"Accept":"application/json"})
            if resp.status_code==200:
                data=resp.json()
                for provider in data.get("results",[]):
                    b=provider.get("basic",{});a=provider.get("addresses",[{}])[0];t=provider.get("taxonomies",[{}])[0]
                    results.append({"npi":provider.get("number",""),"name":f"{b.get('first_name','')} {b.get('last_name','')}".strip() or b.get("organization_name",""),"credential":b.get("credential","")),"specialty":t.get("desc",""),"address":a.get("address_1",""),"city":a.get("city",""),"state":a.get("state",""),"zip":a.get("postal_code",""),"phone":a.get("telephone_number",""),"source":"NPPES NPI Registry"})
        except Exception as e:
            Actor.log.error(f"NPI error: {e}")
    return results


async def main():
    async with Actor:
        config=await Actor.get_input() or{}
        lookup_type=config.get("lookupType","fee_schedule")
        Actor.log.info(f"Lookup type: {lookup_type}")
        all_results=[]
        if lookup_type=="fee_schedule":
            cpt_codes=config.get("cptCodes",["99213"])
            state=config.get("state","CA")
year=config.get("year","2025")
            if isinstance(cpt_codes,str):cpt_codes=[c.strip() for c in cpt_codes.split(",")]
            for cpt in cpt_codes:
                r=await lookup_fee_schedule(cpt.strip(),state,year)
                all_results.extend(r)
        elif lookup_type=="denial_codes":
            codes=config.get("codes",["97"])
            code_type=config.get("codeType","CARC")
            if isinstance(codes,str):codes=[c.strip() for c in codes.split(",")]
            for code in codes:
                r=await lookup_denial_code(code.strip(),code_type)
                all_results.extend(r)
        elif lookup_type=="npi_lookup":
            r=await lookup_npi_provider(config.get("npiNumber"),config.get("providerName"),config.get("specialty"),config.get("state"))
            all_results.extend(r)
        for result in all_results:
            await Actor.push_data(result)
        if all_results:
            await Actor.charge("result-found",len(all_results))
        Actor.log.info(f"Done! {len(all_results)} records.")

if __name__=="__main__":
    asyncio.run(main())
