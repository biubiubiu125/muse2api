import sys,importlib.util,json
from types import SimpleNamespace
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
spec=importlib.util.spec_from_file_location('candidate',sys.argv[1]);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
clock=[0.0]
m.time=SimpleNamespace(time=lambda:clock[0],sleep=lambda s:clock.__setitem__(0,clock[0]+s))
engine=object.__new__(m.MuseEngine)
engine._scroll_bottom=lambda:None
engine.page=SimpleNamespace(js=lambda _:json.dumps({'tail':'Connecting...','cnt':0,'txt':'','stop':True}))
engine.attachments=lambda:[] if clock[0]<20 else [{'src':'generated','tid':'image','w':100,'h':100}]
try:
 result=engine._wait_attachment('',30,'image')
 print('RESULT',result['src'],'elapsed',round(clock[0],1))
 assert result['src']=='generated'
except m.MuseGenerationError as e:
 print('RESULT rejected_before_generated elapsed',round(clock[0],1),'error',str(e))
 if '--assert' in sys.argv:raise
