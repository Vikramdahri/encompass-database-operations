import os, csv, json, time, re, base64, logging, concurrent.futures, threading, mysql.connector
from queue import Queue
from threading import Event

# ============================ CONFIG ============================
MAX_WORKERS = 16
BASE_DIR = "/app/output"
TIMESTAMP = time.strftime("%Y%m%d_%H%M%S")

OUT_DIRS = {
    "json": os.path.join(BASE_DIR, f"json_{TIMESTAMP}"),
    "img": os.path.join(BASE_DIR, f"images_{TIMESTAMP}")
}
for d in OUT_DIRS.values(): os.makedirs(d, exist_ok=True)

FILES = {
    "log": os.path.join(BASE_DIR, f"export_{TIMESTAMP}.log"),
    "status": os.path.join(BASE_DIR, f"status_{TIMESTAMP}.csv"),
    "errors": os.path.join(BASE_DIR, f"errors_{TIMESTAMP}.csv"),
    "summary": os.path.join(BASE_DIR, f"summary_{TIMESTAMP}.json")
}

csv_q, err_q = Queue(), Queue()
counter_lock = threading.Lock()
processed = errors = total_images = peak_mem = 0

# ============================ LOGGING ============================
def setup_log():
    log = logging.getLogger("export"); log.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    fh = logging.FileHandler(FILES["log"], encoding="utf-8"); fh.setFormatter(fmt)
    sh = logging.StreamHandler(); sh.setFormatter(logging.Formatter("%(message)s"))
    log.addHandler(fh); log.addHandler(sh)
    return log
logger = setup_log()

# ============================ DB CONFIG ============================
try:
    from dotenv import load_dotenv; load_dotenv()
except ImportError:
    pass

req = ["DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME"]
if miss := [v for v in req if not os.getenv(v)]:
    raise SystemExit(f"Missing env vars: {', '.join(miss)}")

DB = dict(
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_NAME"),
    autocommit=True, use_pure=True, charset="utf8mb4"
)

# ============================ HELPERS ============================
def get_memory_mib():
    global peak_mem
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    m = int(line.split()[1]) / 1024
                    peak_mem = max(peak_mem, m)
                    return m
    except: pass

def hms(sec):
    h, sec = divmod(int(sec), 3600); m, s = divmod(sec, 60)
    return f"{h:02}:{m:02}:{s:02}"

def connect_db():
    for i in range(5):
        try:
            conn = mysql.connector.connect(**DB)
            conn.ping(reconnect=True, attempts=3, delay=2)
            return conn
        except Exception as e:
            logger.error(f"DB retry {i+1}/5: {e}"); time.sleep(2**i)
    raise SystemExit("DB connection failed")

# ============================ IMAGE HELPERS ============================
def is_base64_image(s):
    if not isinstance(s, str): return False
    if s.startswith("data:image/"): return True
    return any(s.startswith(p) for p in ["iVBORw0KGgo", "/9j/", "R0lGOD"]) and len(s) > 100

def extract_b64_and_ext(val):
    if val.startswith("data:image/"):
        m = re.match(r"data:image/([\w\+]+);base64,(.+)", val)
        if m: return m.group(2), {"jpeg": "jpg", "svg+xml": "svg"}.get(m.group(1), m.group(1))
    try:
        start = base64.b64decode(val[:50])
        if start.startswith(b"\x89PNG"): return val, "png"
        if start.startswith(b"\xff\xd8\xff"): return val, "jpg"
        if start.startswith(b"GIF8"): return val, "gif"
        if b"WEBP" in start[:20]: return val, "webp"
    except: pass
    return val, "png"

def extract_images(obj, rid, parent=None):
    count = 0
    if isinstance(obj, dict):
        name = obj.get("name") or obj.get("filename") or parent
        for k,v in obj.items():
            if is_base64_image(v):
                try:
                    b64, ext = extract_b64_and_ext(v)
                    safe = re.sub(r"[^\w\-_]", "_", name or k)
                    path = os.path.join(OUT_DIRS["img"], f"{rid}_{safe}.{ext}")
                    base, i = path, 1
                    while os.path.exists(path):
                        path = os.path.splitext(base)[0] + f"_{i}.{ext}"; i+=1
                    with open(path,"wb") as f: f.write(base64.b64decode(b64))
                    count += 1
                except Exception as e:
                    logger.debug(f"Image save fail ID {rid}: {e}")
            elif isinstance(v,(dict,list)): count += extract_images(v,rid,name)
    elif isinstance(obj,list):
        for i,it in enumerate(obj):
            if is_base64_image(it):
                try:
                    b64,ext = extract_b64_and_ext(it)
                    safe = re.sub(r"[^\w\-_]", "_", parent or f"item_{i}")
                    path = os.path.join(OUT_DIRS["img"], f"{rid}_{safe}.{ext}")
                    base, j = path, 1
                    while os.path.exists(path):
                        path = os.path.splitext(base)[0]+f"_{j}.{ext}"; j+=1
                    with open(path,"wb") as f: f.write(base64.b64decode(b64))
                    count += 1
                except Exception as e:
                    logger.debug(f"Image list fail {rid}: {e}")
            elif isinstance(it,(dict,list)): count += extract_images(it,rid,parent)
    return count

# ============================ CSV WRITERS ============================
def init_csv():
    for f, hdr in [(FILES["status"],["id","json_saved","images_saved","json_path","error"]),
                   (FILES["errors"],["id","error"])]:
        if not os.path.exists(f):
            with open(f,"w",newline="",encoding="utf-8") as fh: csv.writer(fh).writerow(hdr)

def writer(q, file):
    with open(file,"a",newline="",encoding="utf-8") as fh:
        w = csv.writer(fh)
        while True:
            row = q.get()
            if row is None: break
            w.writerow(row); fh.flush(); q.task_done()

# ============================ RESUME LOGIC ============================
def get_resume_id():
    files = [os.path.join(BASE_DIR,f) for f in os.listdir(BASE_DIR)
             if f.startswith("status_") and f.endswith(".csv") and f!=os.path.basename(FILES["status"])]
    if not files: return 0,None
    latest = max(files,key=os.path.getmtime)
    try:
        with open(latest,"r",encoding="utf-8") as f:
            next(f)
            ids=[int(r.split(",")[0]) for r in f if "True" in r.split(",")[1]]
        rid=max(ids) if ids else 0
        if rid: logger.info(f"Resuming from {rid} ({os.path.basename(latest)})")
        return rid,os.path.basename(latest)
    except: return 0,None

# ============================ HEARTBEAT ============================
def heartbeat(total, stop, start):
    last=0
    while not stop.wait(5):
        done=processed; speed=(done-last)/5; last=done
        eta=(total-done)/speed if speed>0 else 0
        logger.info(f"5s Progress: {done:,}/{total:,} ({done/total*100:5.1f}%) │ {speed:6.1f}/s │ ETA {hms(eta)} │ RAM {get_memory_mib():.1f} MiB")

# ============================ CORE PROCESS ============================
def process_row(rid, blob, row_ts):
    global processed, errors, total_images
    ok=False; imgs=0; err=""
    # Year/Month directories
    json_dir = os.path.join(OUT_DIRS["json"], str(row_ts.year), f"{row_ts.month:02}")
    os.makedirs(json_dir, exist_ok=True)
    path = os.path.join(json_dir, f"{rid}.json")
    try:
        data=json.loads(blob.decode("utf-8"),strict=False)
        imgs=extract_images(data,rid)
        with open(path,"w",encoding="utf-8") as f: json.dump(data,f,indent=2,ensure_ascii=False)
        ok=True
    except Exception as e:
        err=f"{type(e).__name__}: {e}"
    finally:
        csv_q.put([rid,ok,imgs,path if ok else "",err])
        if err: err_q.put([rid,err])
        with counter_lock:
            processed+=1; total_images+=imgs
            if err: errors+=1

# ============================ MAIN ============================
def main():
    global processed, errors
    init_csv()
    conn = connect_db()
    logger.info("DB connected\n")

    start_id, resume = get_resume_id()

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM work_order_steps")
    total = cur.fetchone()[0]
    cur.execute(f"SELECT COUNT(*) FROM work_order_steps WHERE id>{start_id}" if start_id else
                "SELECT COUNT(*) FROM work_order_steps")
    to_export = cur.fetchone()[0]
    logger.info(f"Export start: total {total}, to export {to_export}")

    if not to_export:
        logger.info("Nothing to export.")
        return

    # CSV writers
    writers = concurrent.futures.ThreadPoolExecutor(2)
    writers.submit(writer, csv_q, FILES["status"])
    writers.submit(writer, err_q, FILES["errors"])

    # Heartbeat thread
    stop = Event()
    start_time = time.time()
    heartbeat_thread = threading.Thread(target=heartbeat, args=(to_export, stop, start_time), daemon=True)
    heartbeat_thread.start()

    try:
        cur = conn.cursor(buffered=False)
        q = f"SELECT id, result_steps, timestamp FROM work_order_steps WHERE id>{start_id} ORDER BY id" if start_id else \
            "SELECT id, result_steps, timestamp FROM work_order_steps ORDER BY id"
        cur.execute(q)

        BATCH_SIZE = 500
        with concurrent.futures.ThreadPoolExecutor(MAX_WORKERS) as pool:
            while True:
                rows = cur.fetchmany(BATCH_SIZE)
                if not rows:
                    break
                for rid, blob, ts in rows:
                    pool.submit(process_row, rid, blob, ts)

    finally:
        cur.close()
        conn.close()
        stop.set()
        heartbeat_thread.join()
        csv_q.put(None)
        err_q.put(None)
        writers.shutdown(wait=True)

    elapsed = time.time() - start_time
    uptime = hms(elapsed)

    json_files = sum(len(files) for _, _, files in os.walk(OUT_DIRS["json"]))
    img_files = sum(len(files) for _, _, files in os.walk(OUT_DIRS["img"]))
    csv_files = sum(1 for f in FILES.values() if f.endswith(".csv"))
    log_files = sum(1 for f in FILES.values() if f.endswith(".log"))
    summary_files = sum(1 for f in FILES.values() if f.endswith(".json"))

    logger.info("\n=== Export Completed ===")
    logger.info(f"Uptime: {uptime}")
    logger.info(f"Processed: {processed:,} | Images: {total_images:,} | Errors: {errors:,}")
    logger.info(f"Files Created → JSON: {json_files} | Images: {img_files} | CSV: {csv_files} | Log: {log_files} | Summary: {summary_files}")
    logger.info(f"Peak RAM: {peak_mem:.1f} MiB")

    summary = dict(
        total=total,
        processed=processed,
        resumed=start_id,
        uptime=uptime,
        images=total_images,
        errors=errors,
        elapsed=round(elapsed, 1),
        rps=round(processed/elapsed, 1),
        peak_ram=round(peak_mem, 1),
        files_created=dict(json=json_files, images=img_files, csv=csv_files, log=log_files, summary=summary_files),
        json_dir=OUT_DIRS["json"],
        img_dir=OUT_DIRS["img"]
    )
    with open(FILES["summary"], "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

if __name__=="__main__":
    main()
