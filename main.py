import os
import httpx
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import time

TEST_MODE = False
APP_ID = "1004993"
IDENTIFIER = "<YOUR IDENTIFIER>"
PLATFORM_ID = "<YOUR PLATFORMID>"
HMAC = "<YOUR HMAC>"
MAX_WORKERS = 10

BASE_HEADERS = {
    "Identificador": "Uptodown_Android",
    "Identificador-Version": "731",
    "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 12; ALT-AL10 Build/059d091.0)",
    "Connection": "Keep-Alive",
    "Accept-Encoding": "gzip, deflate"
}

STOP_EVENT = threading.Event()
POS_LOCK = threading.Lock()
AUTH_LOCK = threading.Lock()
WORKER_POSITIONS = {i: False for i in range(MAX_WORKERS)}

def get_pos():
    with POS_LOCK:
        for i in range(MAX_WORKERS):
            if not WORKER_POSITIONS[i]:
                WORKER_POSITIONS[i] = True
                return i
        return MAX_WORKERS

def release_pos(i):
    with POS_LOCK:
        if i in WORKER_POSITIONS:
            WORKER_POSITIONS[i] = False

def authenticate(client):
    auth_url = f"https://www.uptodown.app/eapi/auth/token?identifier={IDENTIFIER}"
    payload = {
        "unixtime": str(int(time.time())),
        "identifier": IDENTIFIER,
        "id_plataforma": PLATFORM_ID,
        "hmac": HMAC,
        "lang": "en"
    }
    try:
        resp = client.post(auth_url, data=payload)
        resp.raise_for_status()
        token = resp.json().get("token")
        if not token:
            raise ValueError()
        return token
    except Exception:
        return None

def refresh_token_if_expired(api_client, current_token):
    with AUTH_LOCK:
        curr_auth = api_client.headers.get("Authorization", "")
        if curr_auth != f"Bearer {current_token}":
            return
        
        setup_client = httpx.Client(headers=BASE_HEADERS)
        new_token = authenticate(setup_client)
        setup_client.close()
        
        if new_token:
            api_client.headers["Authorization"] = f"Bearer {new_token}"
        else:
            print("Authentication refresh failed.")
            exit(1)

def process_stream(r, file_path, version_str, file_name, pos):
    total_size = int(r.headers.get("Content-Length", 0))
    ui_label = f"[{version_str}] {file_name}"
    with open(file_path, "wb") as f, tqdm(
        desc=ui_label[:35].ljust(35),
        total=total_size,
        unit="iB",
        unit_scale=True,
        unit_divisor=1024,
        position=pos,
        leave=False
    ) as bar:
        for chunk in r.iter_bytes(chunk_size=8192):
            if STOP_EVENT.is_set():
                break
            f.write(chunk)
            bar.update(len(chunk))

def download_file(api_client, dl_client, version_str, file_info):
    if STOP_EVENT.is_set():
        return

    version_str = str(version_str).strip()
    file_id = file_info.get("fileID")
    file_name = file_info.get("filename", "").strip()
    
    if not file_name:
        return

    script_dir = os.path.dirname(os.path.abspath(__file__)) if __file__ else os.getcwd()
    version_folder = os.path.join(script_dir, version_str)
    file_path = os.path.join(version_folder, file_name)

    if os.path.isfile(file_path):
        return

    root_file_match = os.path.join(script_dir, file_name)
    if os.path.isfile(root_file_match):
        return

    try:
        for item in os.listdir(script_dir):
            full_item_path = os.path.join(script_dir, item)
            if os.path.isfile(full_item_path):
                if item.startswith(f"{version_str}."):
                    return
    except Exception:
        pass

    os.makedirs(version_folder, exist_ok=True)

    dl_api_url = (
        f"https://www.uptodown.app/eapi/apps/{APP_ID}/file/{file_id}"
        f"/downloadUrl?identifier={IDENTIFIER}&id_plataforma=13&update=0&lang=en"
    )

    try:
        dl_resp = api_client.get(dl_api_url)
        if dl_resp.status_code == 401:
            curr_auth = api_client.headers.get("Authorization", "")
            curr_token = curr_auth.replace("Bearer ", "") if curr_auth.startswith("Bearer ") else ""
            refresh_token_if_expired(api_client, curr_token)
            dl_resp = api_client.get(dl_api_url)
            
        dl_resp.raise_for_status()
        dl_data = dl_resp.json()
        actual_dl_url = dl_data.get("data", {}).get("downloadURL")
    except httpx.HTTPError:
        return

    if not actual_dl_url:
        return

    pos = get_pos()
    try:
        with dl_client.stream("GET", actual_dl_url, follow_redirects=True) as r:
            if r.status_code == 401:
                curr_auth = api_client.headers.get("Authorization", "")
                curr_token = curr_auth.replace("Bearer ", "") if curr_auth.startswith("Bearer ") else ""
                refresh_token_if_expired(api_client, curr_token)
                with dl_client.stream("GET", actual_dl_url, follow_redirects=True) as r_retry:
                    r_retry.raise_for_status()
                    process_stream(r_retry, file_path, version_str, file_name, pos)
            else:
                r.raise_for_status()
                process_stream(r, file_path, version_str, file_name, pos)
    except httpx.HTTPError:
        if os.path.exists(file_path):
            os.remove(file_path)
    finally:
        release_pos(pos)
        if STOP_EVENT.is_set() and os.path.exists(file_path):
            os.remove(file_path)

def main():
    setup_client = httpx.Client(headers=BASE_HEADERS)
    token = authenticate(setup_client)
    setup_client.close()

    if not token:
        print("Initial authentication failed.")
        exit(1)

    api_headers = BASE_HEADERS.copy()
    api_headers["Authorization"] = f"Bearer {token}"

    api_client = httpx.Client(headers=api_headers, timeout=None)
    dl_client = httpx.Client(headers=BASE_HEADERS, timeout=None)
    
    offset = 0
    limit = 20
    page_num = 1
    
    print("Starting Uptodown Version Fetcher...")

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            while not STOP_EVENT.is_set():
                
                versions_url = (
                    f"https://www.uptodown.app/eapi/v3/app/{APP_ID}/device/{IDENTIFIER}"
                    f"/compatible/versions?page[limit]={limit}&identifier={IDENTIFIER}"
                    f"&page[offset]={offset}&id_plataforma=13&lang=en"
                )
                
                try:
                    resp = api_client.get(versions_url)
                    if resp.status_code == 401:
                        curr_auth = api_client.headers.get("Authorization", "")
                        curr_token = curr_auth.replace("Bearer ", "") if curr_auth.startswith("Bearer ") else ""
                        refresh_token_if_expired(api_client, curr_token)
                        resp = api_client.get(versions_url)
                    resp.raise_for_status()
                    data = resp.json().get("data", [])
                except httpx.HTTPError as e:
                    print(f"\nFailed to fetch versions page: {e}")
                    break

                if not data:
                    print("\nNo more versions found or reached the end. Done.")
                    break

                if TEST_MODE:
                    data = data[:1]
                    print("\nTEST MODE ENABLED: Processing exactly 1 version.")

                download_queue = []
                for version_info in data:
                    version_str = version_info.get("version")
                    files = version_info.get("containedFiles", [])

                    if not version_str or not files:
                        continue
                    
                    for file_info in files:
                        download_queue.append({
                            "version_str": version_str,
                            "file_info": file_info,
                            "size": file_info.get("size", float('inf'))
                        })

                download_queue.sort(key=lambda x: x["size"])

                futures = []
                for item in download_queue:
                    futures.append(
                        executor.submit(
                            download_file, api_client, dl_client, item["version_str"], item["file_info"]
                        )
                    )

                for future in as_completed(futures):
                    future.result()

                if TEST_MODE:
                    STOP_EVENT.set()
                    break

                offset += limit
                page_num += 1

    except KeyboardInterrupt:
        STOP_EVENT.set()
        executor.shutdown(wait=False, cancel_futures=True)
        print("\nProcess interrupted via Ctrl+C. Shutting down active streams safely...")

    finally:
        api_client.close()
        dl_client.close()

if __name__ == "__main__":
    main()
