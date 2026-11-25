import json,os,re,urllib.request
from PIL import Image,ImageDraw,ImageFont,ImageFilter
os.chdir('/tmp/km')
idx=json.load(open('idx.json')); cat=json.load(open('catalogo.json'))
url={}
for g,v in idx.items():
    for lab,u in v: url[lab.rstrip('*')]=u
extra={'MC034':'2025/02/NL-MC034-1024x1024.webp','KL028':'2025/02/KL028-1-1024x1024.webp','KL100':'2025/02/KL100-2-1024x1024.webp','KL081':'2025/02/KL081-1-1024x1024.webp',
 'NL058':'2025/02/NL058-1-1024x1024.webp','KL006':'2025/02/KL006-1-1024x1024.webp','NL030':'2025/02/NL030-3-1024x1024.webp','NL170':'2025/02/NL170-1024x1024.webp','KL098':'2025/02/kl098-01-1024x1024.webp','NL073':'2025/02/NL073-1-1024x1024.webp'}
for k,v in extra.items(): url[k]='https://kmlentes.pe/wp-content/uploads/'+v
brand={p['sku']:p['brands'] for p in cat}
def img(lab):
    u=url[lab]; u=re.sub(r'-\d+x\d+(\.\w+)$',r'\1',u) if not lab in extra else u
    fn='src/'+re.sub(r'\W','_',lab)+os.path.splitext(u)[1]
    if not os.path.exists(fn):
        try: open(fn,'wb').write(urllib.request.urlopen(urllib.request.Request(u,headers={'User-Agent':'Mozilla/5.0'}),timeout=60).read())
        except Exception: open(fn,'wb').write(urllib.request.urlopen(urllib.request.Request(url[lab],headers={'User-Agent':'Mozilla/5.0'}),timeout=60).read())
    return Image.open(fn).convert('RGB')
def crop(im,w,h,fy=0.5):
    W,H=im.size
    # quitar la franja del título de arriba (~11 %) y un margen lateral
    im=im.crop((int(W*.04),int(H*.11),int(W*.96),H)); W,H=im.size
    r=w/h
    if W/H>r: nw=int(H*r); x=(W-nw)//2; im=im.crop((x,0,x+nw,H))
    else: nh=int(W/r); y=int((H-nh)*fy); im=im.crop((0,y,W,y+nh))
    return im.resize((w,h),Image.LANCZOS)
F=lambda w,s: ImageFont.truetype(f'fonts/Roboto-{w}.ttf',s)
NAVY=(13,43,69); TEAL=(43,179,163)
def card(name,labs,title,sub,W=1024,H=1024,cols=2,rows=2,badge=None,band=0.2):
    gap=max(6,W//150); bh=int(H*band)
    cv=Image.new('RGB',(W,H),'white')
    cw=(W-gap*(cols-1))//cols; ch=(H-bh-gap*(rows-1))//rows
    for i,l in enumerate(labs[:cols*rows]):
        x=(i%cols)*(cw+gap); y=(i//cols)*(ch+gap)
        cv.paste(crop(img(l),cw,ch,0.35),(x,y))
    d=ImageDraw.Draw(cv)
    d.rectangle((0,H-bh,W,H),fill=NAVY)
    d.rectangle((0,H-bh,W,H-bh+max(4,W//200)),fill=TEAL)
    ts=int(bh*.36); f=F(900,ts)
    while d.textlength(title,font=f)>W*.9: ts-=2; f=F(900,ts)
    fs=F(400,int(bh*.19))
    th=ts; total=th+int(bh*.08)+int(bh*.19)
    y0=H-bh+(bh-total)//2+2
    d.text((W/2,y0),title,font=f,fill='white',anchor='mt')
    d.text((W/2,y0+th+int(bh*.1)),sub,font=fs,fill=(205,238,246),anchor='mt')
    if badge:
        bf=F(700,int(W*.034)); tw=d.textlength(badge,font=bf); pad=int(W*.022); bhh=int(W*.06)
        x0,y0=int(W*.03),int(W*.03)
        d.rounded_rectangle((x0,y0,x0+tw+pad*2+bhh*.55,y0+bhh),radius=bhh//2,fill='white')
        cx,cy,r=x0+pad+bhh*.2,y0+bhh/2,bhh*.24
        d.ellipse((cx-r,cy-r,cx+r,cy+r),fill=TEAL)
        d.line((cx-r*.5,cy,cx-r*.1,cy+r*.45,cx+r*.55,cy-r*.4),fill='white',width=max(3,W//300))
        d.text((x0+pad+bhh*.55,cy),badge,font=bf,fill=NAVY,anchor='lm')
    cv.save(f'gen/{name}.webp',quality=82,method=6); return f'gen/{name}.webp'
def banner(name,labs,W,H,cols,rows):
    gap=max(6,W//200); cv=Image.new('RGB',(W,H),'white')
    cw=(W-gap*(cols-1))//cols; ch=(H-gap*(rows-1))//rows
    for i,l in enumerate(labs[:cols*rows]):
        cv.paste(crop(img(l),cw,ch,0.35),((i%cols)*(cw+gap),(i//cols)*(ch+gap)))
    cv.save(f'gen/{name}.webp',quality=80,method=6)
P=json.load(open('picks.json'))
for k,c in P['cards'].items():
    card(k,c['labs'],c['title'],c['sub'],W=c.get('W',1024),H=c.get('H',1024),badge=c.get('badge'))
for k,b in P['banners'].items(): banner(k,b['labs'],b['W'],b['H'],b['cols'],b['rows'])
print('ok')
