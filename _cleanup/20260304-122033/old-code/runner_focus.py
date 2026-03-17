import os, sys, re, json, time, shutil, hashlib, random
from pathlib import Path
from urllib.parse import urljoin, urlparse
import urllib.request

# ======================================================
# PolicyQueue Focus Runner
# Focus: Kansas TAX REVENUE + STAR BONDS only
# ======================================================

ROOT = Path(__file__).resolve().parent

INBOX   = ROOT / "inbox"
ARCHIVE = ROOT / "archive"
FACTS   = ROOT / "facts"
QUEUE   = ROOT / "queue"
LOGS    = ROOT / "logs"

RUNLOG = LOGS / "runner.log"

OLLAMA_BASE = os.environ.get("PQ_OLLAMA_BASE","http://127.0.0.1:11434")
MODEL       = os.environ.get("PQ_MODEL","llama3.1:8b-instruct-q4_K_M")

FOCUS_KEYWORDS = [
    "star bond",
    "star bonds",
    "sales tax",
    "tax revenue",
    "revenue estimate",
    "revenue report",
    "department of revenue",
    "tax collections",
    "income tax",
    "sales tax receipts"
]

DOMAINS = [
    "ksrevenue.gov",
    "budget.kansas.gov",
    "kslegislature.org",
    "ksrevisor.org",
    "postaudit.ks.gov"
]

SEEDS = [
    "https://www.ksrevenue.gov/",
    "https://budget.kansas.gov/",
    "https://kslegislature.org/"
]

def log(msg):
    LOGS.mkdir(exist_ok=True)
    with open(RUNLOG,"a",encoding="utf8") as f:
        f.write(time.strftime("%Y-%m-%d %H:%M:%S ") + msg + "\n")

def extract_links(html,base):
    out=[]
    for m in re.finditer(r'href=["\']([^"\']+)["\']',html,re.I):
        u=urljoin(base,m.group(1))
        out.append(u)
    return out

def allowed(url):
    d=urlparse(url).netloc.lower()
    return any(d.endswith(x) for x in DOMAINS)

def is_pdf(url):
    return ".pdf" in url.lower()

def fetch(url):
    req=urllib.request.Request(url,headers={"User-Agent":"PolicyQueue"})
    with urllib.request.urlopen(req,timeout=25) as r:
        return r.read()

def pdf_to_text(path):
    import fitz
    doc=fitz.open(str(path))
    out=[]
    for p in doc:
        out.append(p.get_text())
    return "\n".join(out)

def focus_hit(name,text):
    t=(name+text).lower()
    return any(k in t for k in FOCUS_KEYWORDS)

def ollama(prompt):
    import urllib.request,json
    data=json.dumps({"model":MODEL,"prompt":prompt,"stream":False}).encode()
    req=urllib.request.Request(
        OLLAMA_BASE+"/api/generate",
        data=data,
        headers={"Content-Type":"application/json"}
    )
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())["response"]

def make_post(text,url):
    prompt=f"""
Write a short factual post under 700 characters.

Topic must be Kansas tax revenue or STAR bonds.

Include ONE short quote from the document.

Source: {url}

TEXT:
{text[:5000]}
"""
    out=ollama(prompt).strip()
    if "Source:" not in out:
        return ""
    return out

def process(pdf,url):
    text=pdf_to_text(pdf)

    if not focus_hit(pdf.name,text[:10000]):
        log(f"SKIP {pdf.name} not focus")
        shutil.move(str(pdf),ARCHIVE/pdf.name)
        return

    post=make_post(text,url)

    if not post:
        log(f"SKIP {pdf.name} writer empty")
        shutil.move(str(pdf),ARCHIVE/pdf.name)
        return

    QUEUE.mkdir(exist_ok=True)
    out=QUEUE/(pdf.stem+".post.txt")
    out.write_text(post,encoding="utf8")

    shutil.move(str(pdf),ARCHIVE/pdf.name)

    log(f"QUEUED {out.name}")

def crawl():
    saved=0

    for seed in SEEDS:
        try:
            html=fetch(seed).decode("utf8","ignore")
        except:
            continue

        for link in extract_links(html,seed):

            if not allowed(link):
                continue

            if not is_pdf(link):
                continue

            try:
                data=fetch(link)
            except:
                continue

            name=link.split("/")[-1]

            INBOX.mkdir(exist_ok=True)

            path=INBOX/name

            with open(path,"wb") as f:
                f.write(data)

            saved+=1

    log(f"CRAWL saved {saved} pdfs")

def run_once():

    crawl()

    for pdf in INBOX.glob("*.pdf"):
        process(pdf,"UNKNOWN")

def main():

    cmd=sys.argv[1] if len(sys.argv)>1 else "loop"

    if cmd=="once":
        run_once()
        return

    while True:
        run_once()
        time.sleep(300)

if __name__=="__main__":
    main()