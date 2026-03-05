import asyncio
import aiohttp
from bs4 import BeautifulSoup
import json
import os
import re
import pandas as pd
from datetime import datetime, timezone
import sys
import random
import time

# --- WINDOWS ENCODING FIX ---
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')

# --- CONFIGURATION ---
INPUT_FILE = "validated_channels.xlsx"
OUTPUT_FILE = "raw_intel.json"
CACHE_FILE = "seen_tg_links.json"
ERROR_LOG = "scraper_errors.json"

MAX_CONCURRENT_REQUESTS = 5      # To avoid Telegram IP bans
MAX_STORAGE_LIMIT = 50000        # FIFO limit for the JSON database
MAX_PAGES_PER_CHANNEL = 8        # Deep history pagination (approx 160 messages per channel)
REQUEST_DELAY = 1.2              # Seconds between page requests per channel

# --- 2026 VALIDATED SELECTORS ---
SELECTORS = {
    "message_wrap": "tgme_widget_message_wrap",
    "message_text": "tgme_widget_message_text",
    "message_date": "tgme_widget_message_date",
    "bubble": "tgme_widget_message_bubble"
}

# --- UTILITIES ---
def clean_text(text):
    if not text: return ""
    text = re.sub('<[^<]+?>', '', str(text)) # Strip HTML
    return " ".join(text.split()).strip()

def extract_post_id(url):
    if not url: return None
    try:
        # Format: https://t.me/s/channel/12345
        return int(url.rstrip('/').split('/')[-1])
    except: return None

def get_relative_time(ts_iso):
    try:
        past = datetime.fromisoformat(ts_iso.replace('Z', '+00:00')).replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        diff = now - past
        minutes = int(diff.total_seconds() / 60)
        if minutes < 60: return f"{minutes}m ago"
        hours = int(minutes / 60)
        if hours < 24: return f"{hours}h ago"
        return f"{int(hours/24)}d ago"
    except: return "Recent"

# --- CORE SCRAPING ENGINE ---
async def fetch_with_retry(session, url, attempt=0):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    }
    try:
        async with session.get(url, headers=headers, timeout=15) as resp:
            if resp.status == 200:
                return await resp.text(), 200
            elif resp.status == 429:
                wait = (2 ** attempt) + random.uniform(5, 10)
                print(f"⏳ Rate limited. Waiting {wait:.1f}s...")
                await asyncio.sleep(wait)
                return await fetch_with_retry(session, url, attempt + 1)
            return None, resp.status
    except Exception as e:
        return None, -1

async def scrape_tg_channel(session, channel_url, sem, seen_links, is_first_run, errors):
    async with sem:
        handle = str(channel_url).split('/')[-1].replace('@', '').strip()
        if not handle or len(handle) < 3: return []

        print(f"📡 Scanning: @{handle}")
        all_channel_intel = []
        current_before = None
        pages_to_fetch = MAX_PAGES_PER_CHANNEL if is_first_run else 3

        for page_idx in range(pages_to_fetch):
            url = f"https://t.me/s/{handle}"
            if current_before: url += f"?before={current_before}"
            
            html, status = await fetch_with_retry(session, url)
            if status != 200 or not html:
                if status != -1: errors.append({"handle": handle, "status": status})
                break
            
            soup = BeautifulSoup(html, 'html.parser')
            wraps = soup.find_all('div', class_=SELECTORS["message_wrap"])
            
            if not wraps: break

            min_id_on_page = None
            for wrap in wraps:
                date_elem = wrap.find('a', class_=SELECTORS["message_date"])
                if not date_elem: continue
                
                link = date_elem.get('href', '')
                if link in seen_links: continue
                
                # Track post ID for pagination logic
                p_id = extract_post_id(link)
                if p_id:
                    if min_id_on_page is None or p_id < min_id_on_page:
                        min_id_on_page = p_id
                
                # Content Extraction
                text_div = wrap.find('div', class_=SELECTORS["message_text"])
                if not text_div:
                    text_div = wrap.find('div', class_=SELECTORS["bubble"])
                
                content = clean_text(text_div.get_text(separator=' ')) if text_div else ""
                if len(content) < 15: continue # Skip short/noise/media-only

                time_elem = wrap.find('time')
                ts = time_elem.get('datetime') if time_elem else datetime.now(timezone.utc).isoformat()
                
                all_channel_intel.append({
                    "source": f"TG/@{handle}",
                    "content": content[:2000],
                    "link": link,
                    "timestamp": ts,
                    "human_time": get_relative_time(ts)
                })
                seen_links.add(link)

            # Check if we can paginate further back
            if min_id_on_page and min_id_on_page > 1:
                current_before = min_id_on_page
            else:
                break
            
            await asyncio.sleep(REQUEST_DELAY)

        return all_channel_intel

async def main():
    start_time = time.time()
    print(f"🔥 [Ignition] NarrativeOS Telegram Scraper - {datetime.now().strftime('%H:%M:%S')}")

    # 1. Load Sources
    try:
        df = pd.read_excel(INPUT_FILE)
        # Find column automatically
        target_col = [c for c in df.columns if any(w in str(c).lower() for w in ['handle', 'channel', 'telegram'])][0]
        channels = df[target_col].dropna().unique().tolist()
        print(f"📡 Loaded {len(channels)} unique channels.")
    except Exception as e:
        print(f"❌ Load Error: {e}"); return

    # 2. State Management
    is_first_run = not os.path.exists(CACHE_FILE)
    seen_links = set(json.load(open(CACHE_FILE)) if not is_first_run else [])
    db = json.load(open(OUTPUT_FILE, 'r', encoding='utf-8')) if os.path.exists(OUTPUT_FILE) else []
    
    errors = []
    sem = asyncio.BoundedSemaphore(MAX_CONCURRENT_REQUESTS)

    # 3. Execution
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS, ttl_dns_cache=600)
    async with aiohttp.ClientSession(connector=connector) as session:
        # Process in smaller batches of 20 to manage memory/IP health
        BATCH_SIZE = 20
        all_new_intel = []

        for i in range(0, len(channels), BATCH_SIZE):
            batch = channels[i : i + BATCH_SIZE]
            print(f"📦 Processing Batch {i//BATCH_SIZE + 1}...")
            
            tasks = [scrape_tg_channel(session, ch, sem, seen_links, is_first_run, errors) for ch in batch]
            results = await asyncio.gather(*tasks)
            
            batch_data = [item for sublist in results for item in sublist]
            all_new_intel.extend(batch_data)
            
            if i + BATCH_SIZE < len(channels):
                await asyncio.sleep(5) # Cool down between batches

    # 4. FIFO Rotation & Save
    if all_new_intel:
        # Combine and Keep newest
        combined = all_new_intel + db
        # Final sort by timestamp
        combined.sort(key=lambda x: x['timestamp'] or '1970-01-01', reverse=True)
        db = combined[:MAX_STORAGE_LIMIT]
        
        with open(OUTPUT_FILE, "w", encoding='utf-8') as f:
            json.dump(db, f, indent=4, ensure_ascii=False)
        
        with open(CACHE_FILE, "w", encoding='utf-8') as f:
            # Maintain 100K links in cache
            json.dump(list(seen_links)[-100000:], f, indent=4)
        
    if errors:
        with open(ERROR_LOG, "w") as f: json.dump(errors, f, indent=2)

    print(f"✅ Mission Complete. Added {len(all_new_intel)} packets.")
    print(f"⏱️ Total Mission Time: {(time.time() - start_time)/60:.2f} minutes.")

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())