import os,json
log=r'D:\output\run001_a2\co2_only_7_pressure_no_write\run_20260504_102409\run.log'
ls=open(log,encoding='utf-8').readlines()
pts=[json.loads(l)['message'] for l in ls if 'Point' in l and 'sampled' in l]
print(f'{len(ls)} lines, {len(pts)} points')
print('---')
for p in pts:
    print(p[:180])
print('---LAST---')
for l in ls[-2:]:
    print(json.loads(l)['message'][:150])
