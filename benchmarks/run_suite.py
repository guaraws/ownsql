#!/usr/bin/env python3
"""Five alternating independent analytical rounds, exact results, bootstrap CI.
Requires built release binaries and running local servers. No secrets in artifacts.
"""
import argparse, csv, hashlib, json, os, pathlib, platform, random, statistics, subprocess, time
ROOT=pathlib.Path(__file__).resolve().parents[1]

def run(args,engine=None,**kw):
    env=dict(os.environ)
    if engine: env['OWNSQL_BENCH_URL']=os.environ['OWNSQL_'+engine.upper()+'_URL']
    return subprocess.run([str(ROOT/'target/release/bench'),*map(str,args)],cwd=ROOT,env=env,check=True,**kw)

def quantile(a,p):
    a=sorted(a);return a[max(0,min(len(a)-1,int(len(a)*p+0.999999)-1))]

def ci(medians):
    rng=random.Random(42)
    boot=sorted(statistics.median(rng.choices(medians,k=len(medians))) for _ in range(10000))
    return [quantile(boot,.025),quantile(boot,.975)]

def summarize(folder,manifest):
    groups={}
    for path in folder.glob('round-*/**/samples.jsonl'):
        parent=json.loads((path.parent/'manifest.json').read_text())
        for line in path.read_text().splitlines():
            row=json.loads(line);groups.setdefault((parent['engine'],row['query_id']),[]).append((path.parent.name,row))
    table=[]
    for (engine,query),rows in sorted(groups.items()):
        values=[r['total_ms'] for _,r in rows if r['success']]
        repetitions={}
        for repeat,r in rows:
            if r['success']:repetitions.setdefault(repeat,[]).append(r['total_ms'])
        medians=[statistics.median(v) for v in repetitions.values()]
        entry={'engine':engine,'query':query,'successful_ops':len(values),'errors':len(rows)-len(values),'repetitions':len(medians),'p50_ms':quantile(values,.5) if values else None,'p95_ms':quantile(values,.95) if values else None,'p99_ms':quantile(values,.99) if len(values)>=1000 else None,'median_ci95_ms':ci(medians) if medians else None}
        table.append(entry)
    (folder/'summary.json').write_text(json.dumps(table,indent=2))
    md='# Benchmarks CPU versus PostgreSQL\n\nBuild release; mesma conexão simple-query e consumo completo dos resultados. Cinco rodadas alternadas, seed 42, warm cache. CI95: bootstrap de 10.000 reamostragens das medianas por rodada; com cinco rodadas, a incerteza permanece ampla. Erros são mantidos. p99 ausente abaixo de mil observações.\n\n'
    md+=f"Dataset: {manifest['dataset']['rows']:,} linhas. Hash: `{manifest['dataset']['sha256']}`.\n\n"
    md+='| Motor | Consulta | Sucessos | Erros | p50 ms | p95 ms | CI95 mediana ms |\n|---|---|---:|---:|---:|---:|---|\n'
    for r in table:
        ci95='N/A' if not r['median_ci95_ms'] else f"{r['median_ci95_ms'][0]:.3f}–{r['median_ci95_ms'][1]:.3f}"
        md+=f"| {r['engine']} | {r['query']} | {r['successful_ops']} | {r['errors']} | {r['p50_ms']:.3f} | {r['p95_ms']:.3f} | {ci95} |\n"
    md+='\nÍndices não são implementados pelo OwnSQL; nenhuma PK é declarada no dataset analítico e PostgreSQL também usa scans. B01 aqui é busca por ID sem índice, não o gate B01 indexado da especificação. RAM/VRAM, escrita sustentada, cold cache, open-loop e outros workloads exigem rodadas separadas. Nenhum resultado CUDA/HIP é produzido.\n'
    (folder/'report.md').write_text(md)
    return table

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--rows',type=int,default=100000);ap.add_argument('--samples',type=int,default=30);ap.add_argument('--repetitions',type=int,default=5);ap.add_argument('--output',default='benchmarks/results/suites');a=ap.parse_args()
    assert a.repetitions>=5 and a.samples>=30
    for engine in ['cpu','postgres']:assert os.environ.get('OWNSQL_'+engine.upper()+'_URL'),f'set OWNSQL_{engine.upper()}_URL'
    folder=ROOT/a.output/f'{int(time.time())}-{a.rows}';folder.mkdir(parents=True)
    data=folder/'dataset';run(['generate','--seed',42,'--rows',a.rows,'--output',data])
    manifest={'started_utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),'dataset':json.loads((data/'manifest.json').read_text()),'repetitions':a.repetitions,'samples_per_round':a.samples,'build':'release','rust':subprocess.check_output(['rustc','--version'],cwd=ROOT,text=True).strip(),'platform':platform.platform(),'thread_budget_cpu':4,'cache':'warm; no OS cache purge','credentials':'environment; omitted','source_hash':hashlib.sha256(b''.join(p.read_bytes() for p in sorted((ROOT/'crates').glob('*/src/*.rs')))).hexdigest(),'load_seconds':{}}
    (folder/'manifest.json').write_text(json.dumps(manifest,indent=2))
    for engine in ['postgres','cpu']:
        start=time.monotonic();run(['load','--engine',engine,'--input',data/'events.sql'],engine);manifest['load_seconds'][engine]=time.monotonic()-start
    (folder/'manifest.json').write_text(json.dumps(manifest,indent=2))
    failed=[]
    for repetition in range(a.repetitions):
        order=['postgres','cpu'] if repetition%2==0 else ['cpu','postgres']
        for engine in order:
            dest=folder/f'round-{repetition+1}';dest.mkdir(exist_ok=True)
            try:run(['run','--engine',engine,'--samples',a.samples,'--dataset',data/'manifest.json','--output',dest],engine)
            except subprocess.CalledProcessError as e:failed.append({'round':repetition+1,'engine':engine,'exit_code':e.returncode})
        runs=list(dest.glob('*/results.json'));results={json.loads((p.parent/'manifest.json').read_text())['engine']:json.loads(p.read_text()) for p in runs}
        if results.get('cpu')!=results.get('postgres'):failed.append({'round':repetition+1,'error':'differential result mismatch'})
    manifest['failures']=failed;manifest['finished_utc']=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime());(folder/'manifest.json').write_text(json.dumps(manifest,indent=2));summarize(folder,manifest)
    print(f'Suite report: {folder}/report.md');assert not failed,failed
if __name__=='__main__':main()
