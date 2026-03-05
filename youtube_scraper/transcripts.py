import os
import re
import time
import subprocess
import tempfile
import concurrent.futures
import threading
from .utils import save_progress

print_lock = threading.Lock()
save_lock = threading.Lock()


def parse_vtt(vtt_path):
    """Parse a .vtt subtitle file into plain text, removing duplicates.
    
    Args:
        vtt_path (str): The local path to the generated .vtt file.
        
    Returns:
        str: The clean parsed transcript text.
    """
    lines = []
    seen = set()

    with open(vtt_path, "r", encoding="utf-8") as f:
        content = f.read()

    blocks = content.strip().split("\n\n")
    for block in blocks:
        block_lines = block.strip().split("\n")
        for line in block_lines:
            line = line.strip()
            if (not line
                    or line.startswith("WEBVTT")
                    or line.startswith("Kind:")
                    or line.startswith("Language:")
                    or "-->" in line
                    or line.isdigit()):
                continue
            line = re.sub(r"<[^>]+>", "", line).strip()
            if line and line not in seen:
                seen.add(line)
                lines.append(line)

    return " ".join(lines)


def get_transcript_ytdlp(video_id, target_languages, cookies_file=None):
    """Fetch transcript using yt-dlp with cookies and remote components.
    
    Args:
        video_id (str): The YouTube video ID.
        target_languages (list): List of language codes to download.
        cookies_file (str, optional): Path to cookies.txt. Defaults to None.
        
    Returns:
        tuple: (text, language, error, was_rate_limited)
    """
    url = f"https://www.youtube.com/watch?v={video_id}"
    
    # Safely format languages for yt-dlp
    if isinstance(target_languages, list) and len(target_languages) > 0:
        langs_str = ",".join(str(lang) for lang in target_languages)
    else:
        langs_str = "en.*"

    with tempfile.TemporaryDirectory() as tmpdir:
        for sub_type in ["--write-auto-subs", "--write-subs"]:
            cmd = [
                "yt-dlp",
                "--remote-components", "ejs:github",
            ]
            if cookies_file and os.path.exists(cookies_file):
                cmd.extend(["--cookies", cookies_file])

            cmd.extend([
                sub_type,
                "--sub-langs", langs_str,
                "--sub-format", "vtt",
                "--skip-download",
                "--no-playlist",
                "--ignore-errors",
                "-o", os.path.join(tmpdir, "%(id)s.%(ext)s"),
                url
            ])

            try:
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=60
                )

                output = result.stdout + result.stderr

                # Check for rate limiting
                if "429" in output:
                    return None, None, "rate_limited_429", True

                # Find any .vtt file in tmpdir
                vtt_files = [f for f in os.listdir(tmpdir) if f.endswith(".vtt")]

                if vtt_files:
                    vtt_path = os.path.join(tmpdir, vtt_files[0])
                    text = parse_vtt(vtt_path)
                    if text:
                        parts = vtt_files[0].split(".")
                        lang = parts[-2] if len(parts) >= 3 else "en"
                        return text, lang, None, False

            except subprocess.TimeoutExpired:
                return None, None, "timeout", False
            except Exception as e:
                return None, None, str(e), False

    return None, None, "no_subtitles_found", False


def add_transcripts(videos, target_video_ids, target_languages, output_file, source_target, cookies_file=None, base_delay=4, max_delay=60, backoff_multiplier=2, export_format="json", workers=1):
    """Add transcripts only for specified videos with progressive rate-limit backing off and parallel workers.
    
    Args:
        videos (list): Full list of videos loaded from JSON.
        target_video_ids (list): Video IDs we explicitly want to process this run.
        target_languages (list): Language codes to request.
        output_file (str): Path to output JSON to save progress constantly.
        source_target (str): ID of the overall target (for the JSON wrapper).
        cookies_file (str): Optional path to cookies.txt.
        base_delay (int): Minimum seconds between videos.
        max_delay (int): Maximum back-off penalty.
        backoff_multiplier (float): Multiplier for 429 timeouts.
        export_format (str): Desired export format (json, jsonl, csv, parquet).
        workers (int): Number of parallel download threads.
        
    Returns:
        list: The updated master videos list.
    """
    
    videos_to_process = [v for v in videos if v["id"] in target_video_ids]
    total = len(videos_to_process)
    already_done = sum(1 for v in videos_to_process if v.get("transcript") is not None)
    
    if total == 0:
        return videos
        
    print(f"Fetching transcripts for {total} targeted videos using yt-dlp...")
    print(f"Already have: {already_done} | Still needed: {total - already_done}")
    print(f"Workers: {workers} | Base delay: {base_delay}s between videos. Will back off on 429s.\n")

    rate_limit_state = {"current_delay": base_delay, "consecutive_429s": 0}
    delay_lock = threading.Lock()

    def process_video(args):
        idx, video = args

        # SKIP if transcript was successfully fetched
        if video.get("transcript") is not None:
            with print_lock:
                print(f"  [{idx+1}/{total}] SKIP (have transcript): {video['title'][:50]}")
            return

        # Reset any previous error so we retry
        video["transcript_error"] = None
        video_id = video["id"]
        title = video["title"][:55]

        # Respect dynamic delay tracking if we are currently rate-limited
        with delay_lock:
            local_delay = rate_limit_state["current_delay"]
            
        if local_delay > base_delay:
            time.sleep(local_delay)

        text, lang, error, was_429 = get_transcript_ytdlp(
            video_id, 
            target_languages=target_languages,
            cookies_file=cookies_file
        )

        with delay_lock:
            if was_429:
                rate_limit_state["consecutive_429s"] += 1
                rate_limit_state["current_delay"] = min(rate_limit_state["current_delay"] * backoff_multiplier, max_delay)
                video["transcript_error"] = "rate_limited_429"
                with print_lock:
                    print(f"  [{idx+1}/{total}] ⚠ 429 RATE LIMITED: {title}")
                    print(f"      → Backing off. New delay: {rate_limit_state['current_delay']}s (hit {rate_limit_state['consecutive_429s']}x in a row)")
            else:
                if rate_limit_state["consecutive_429s"] > 0:
                    rate_limit_state["current_delay"] = max(rate_limit_state["current_delay"] // backoff_multiplier, base_delay)
                    rate_limit_state["consecutive_429s"] = 0

                video["transcript"] = text
                video["transcript_language"] = lang
                video["transcript_error"] = error

                status = f"✓ ({lang})" if text else f"✗ {error}"
                with print_lock:
                    print(f"  [{idx+1}/{total}] {status}: {title}")

        # Save safely after every video
        with save_lock:
            save_progress(output_file, source_target, videos, export_format=export_format)
            
        with delay_lock:
            local_delay = rate_limit_state["current_delay"]
        time.sleep(local_delay)

    # Launch threads safely with submit() instead of map()
    futures = []
    if workers > 1:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            for args in enumerate(videos_to_process):
                futures.append(executor.submit(process_video, args))
            
            # Optionally wait for all to complete if you want to handle exceptions 
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    with print_lock:
                        print(f"⚠️ Worker Thread Exception: {e}")
    else:
        for args in enumerate(videos_to_process):
            process_video(args)

    return videos
