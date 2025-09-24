import os
import sys
import time
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

INDEX_URL = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2024/index.html"
OUTPUT_DIR = "ais_2024"
MAX_WORKERS = 6
RETRIES = 3
TIMEOUT = (10, 60)
PAUSE_BETWEEN_TASKS = 0.2
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; AISDownloader/1.0; +https://no-url)"}


def get_zip_links(index_url):
    """Returns a list (without duplicates) of absolute URLs to .zip files in the index."""
    r = requests.get(index_url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    hrefs = [a.get("href") for a in soup.find_all("a", href=True)]

    links_abs = []
    vistos = set()
    for href in hrefs:
        if not href:
            continue
        if href.lower().endswith(".zip"):
            url_abs = urljoin(index_url, href)
            if url_abs not in vistos:
                vistos.add(url_abs)
                links_abs.append(url_abs)
    return links_abs


def _head_content_length(session, url):
    try:
        h = session.head(url, allow_redirects=True, headers=HEADERS, timeout=TIMEOUT)
        cl = h.headers.get("content-length")
        return int(cl) if cl is not None else None
    except Exception:
        return None


def download_one(url, out_dir, pbar=None):
    """Download a file. Skip if already exists with the same remote size."""
    nombre = url.split("/")[-1]
    destino = os.path.join(out_dir, nombre)

    with requests.Session() as s:
        s.headers.update(HEADERS)
        tam_remoto = _head_content_length(s, url)

        if (
            os.path.exists(destino)
            and tam_remoto
            and os.path.getsize(destino) == tam_remoto
        ):
            if pbar:
                pbar.update(1)
            return nombre, "skipped (already exists with the same remote size)"

        for intento in range(1, RETRIES + 1):
            try:
                with s.get(url, stream=True, timeout=TIMEOUT) as r:
                    r.raise_for_status()
                    temporal = destino + ".part"
                    with open(temporal, "wb") as f:
                        for chunk in r.iter_content(chunk_size=1024 * 1024):
                            if chunk:
                                f.write(chunk)
                    os.replace(temporal, destino)
                if pbar:
                    pbar.update(1)
                return nombre, "ok"
            except Exception as e:
                if intento == RETRIES:
                    if pbar:
                        pbar.update(1)
                    return nombre, f"failed: {e}"
                time.sleep(1.5 * intento)


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"Buscando .zip en: {INDEX_URL}")
    urls = get_zip_links(INDEX_URL)
    if not urls:
        print("No .zip files found in the index. Check the URL.")
        sys.exit(1)

    print(f"Found {len(urls)} .zip files")
    resultados = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool, tqdm(
        total=len(urls), unit="file"
    ) as pbar:
        futures = [pool.submit(download_one, u, OUTPUT_DIR, pbar) for u in urls]
        for fut in as_completed(futures):
            resultados.append(fut.result())
            time.sleep(PAUSE_BETWEEN_TASKS)

    ok = sum(1 for _, st in resultados if st == "ok")
    salt = sum(1 for _, st in resultados if st.startswith("skipped"))
    fall = [(n, st) for n, st in resultados if st.startswith("falló")]

    print(
        f"\nSummary → ✓ Downloaded: {ok}  •  ⏭ Skipped: {salt}  •  ✗ Failed: {len(fall)}"
    )
    if fall:
        print("Examples of failed:")
        for n, st in fall[:10]:
            print(f" - {n}: {st}")


if __name__ == "__main__":
    main()
