import json, os
def load_txt(p):
    st = ""
    rds = 0
    trms = {}
    if not os.path.exists(p): return None
    for l in open(p):
        if "Status:" in l: st = l.split(":",1)[1].strip()
        elif "Total Rounds:" in l: rds = int(l.split(":",1)[1].strip())
        elif l.startswith("- ") and ":" in l:
            k,v = l[2:].split(":",1)
            try: trms[k.strip()] = float(v.strip().replace("$","").replace(",",""))
            except: trms[k.strip()] = v.strip()
    return {"st":st, "rds":rds, "trms":trms}

prods = json.load(open("products.json"))
pdict = {p["id"]: p for p in prods}

res = []
for i in range(1, 52):
    p0 = f"outputs/product_{i}.txt"
    p1 = f"outputs1/product_{i}.txt"
    if os.path.exists(p0) and os.path.exists(p1):
        o0 = load_txt(p0)
        o1 = load_txt(p1)
        p = pdict[i]
        ret = float(str(p["Retail Price"]).replace("$","").replace(",",""))
        who = float(str(p["Wholesale Price"]).replace("$","").replace(",",""))
        mid = (ret+who)/2.0
        v0 = o0["trms"].get("unit_price_usd", None) if o0 else None
        v1 = o1["trms"].get("unit_price_usd", None) if o1 else None
        
        print(f"ID {i} Mid {mid} 0:{v0} rounds:{o0['rds']} 1:{v1} rounds:{o1['rds']}")
        
        res.append({
            "mid": mid, "r": ret, "w": who,
            "0": {"p": v0, "rds": o0["rds"], "st": o0["st"]},
            "1": {"p": v1, "rds": o1["rds"], "st": o1["st"]}
        })

print(f"Data N: {len(res)}")
def get_p(item, k): return item[k]["p"]
def dev(item, k): p = get_p(item, k); return abs(p - item["mid"]) if p is not None else None
def dev_list(d, k): return [dev(x,k) for x in d if get_p(x,k) is not None]

s0 = dev_list(res, "0")
s1 = dev_list(res, "1")
print(f"0 (No Guard): Rds Mean: {sum(x['0']['rds'] for x in res)/len(res)}, Dev Mean: {sum(s0)/len(s0)}, Dev Worst: {max(s0)}")
print(f"1 (Guard): Rds Mean: {sum(x['1']['rds'] for x in res)/len(res)}, Dev Mean: {sum(s1)/len(s1)}, Dev Worst: {max(s1)}")
