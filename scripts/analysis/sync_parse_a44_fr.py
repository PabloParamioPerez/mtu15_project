"""Sync + parse + build A44 French DA prices, 2018-01 → 2026-04."""
from __future__ import annotations
import sys
import time
sys.path.insert(0, 'src')
import requests
from pathlib import Path
import pandas as pd
from mtu.ingestion.entsoe_common import USER_AGENT, load_token, month_chunks, fetch_document
from mtu.parsing.entsoe.da_price import parse_xml_bytes

FR_EIC = '10YFR-RTE------C'
OUT_RAW = Path('data/raw/entsoe/prices/fr_da')
OUT_PROC = Path('data/processed/entsoe/prices/fr_da')
OUT_ALL = Path('data/processed/entsoe/prices/fr_da_all.parquet')
OUT_RAW.mkdir(parents=True, exist_ok=True)
OUT_PROC.mkdir(parents=True, exist_ok=True)

token = load_token()
s = requests.Session()
s.headers.update({'User-Agent': USER_AGENT})

t0 = time.time()
chunks = list(month_chunks('2018-01','2026-04'))
print(f'Downloading {len(chunks)} monthly A44 FR files...')

for yyyymm, ps, pe in chunks:
    fn = OUT_RAW / f'a44_fr_{yyyymm}.xml'
    if fn.exists() and fn.stat().st_size > 500:
        continue
    params = {
        'documentType':'A44',
        'in_Domain': FR_EIC,
        'out_Domain': FR_EIC,
        'periodStart': ps.replace('-','')+'0000',
        'periodEnd':   pe.replace('-','')+'0000',
    }
    body, status = fetch_document(session=s, token=token, params=params, timeout=300)
    tmp = fn.with_suffix('.part')
    tmp.write_bytes(body)
    tmp.replace(fn)

print(f'Downloaded in {time.time()-t0:.1f}s; parsing...')

all_dfs = []
for xml_path in sorted(OUT_RAW.glob('a44_fr_*.xml')):
    df = parse_xml_bytes(xml_path.read_bytes(),
                          source_file=xml_path.name, domain_label='FR')
    if not df.empty:
        out = OUT_PROC / (xml_path.stem + '.parquet')
        df.to_parquet(out, index=False)
        all_dfs.append(df)

combined = pd.concat(all_dfs, ignore_index=True).sort_values(['isp_start_utc'])
combined.to_parquet(OUT_ALL, index=False)
print(f'Wrote {OUT_ALL} — {len(combined):,} rows, {combined["isp_start_utc"].min()} → {combined["isp_start_utc"].max()}')
print(f'total: {time.time()-t0:.1f}s')
