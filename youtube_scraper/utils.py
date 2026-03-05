import json
import os
import re
import csv

def clean_youtube_input(url):
    """Clean and extract IDs from messy YouTube URLs.
    
    Returns:
        tuple: (target_type, clean_id)
        Where target_type is 'video', 'playlist', 'channel', or None.
    """
    url = url.strip()
    
    # 1. Video matching
    if "youtu.be/" in url:
        return "video", url.split("youtu.be/")[1].split("?")[0]
    if "watch?v=" in url:
        match = re.search(r"v=([a-zA-Z0-9_-]+)", url)
        if match:
            # check if a list is also present
            if "list=" in url:
                lmatch = re.search(r"list=([a-zA-Z0-9_-]+)", url)
                if lmatch:
                    return "playlist", lmatch.group(1)
            return "video", match.group(1)
    if "/shorts/" in url:
        return "video", url.split("/shorts/")[1].split("?")[0]
        
    # 2. Playlist matching
    if "list=" in url:
        match = re.search(r"list=([a-zA-Z0-9_-]+)", url)
        if match:
            return "playlist", match.group(1)
            
    # 3. Channel matching
    if "/channel/" in url:
        return "channel", url.split("/channel/")[1].split("?")[0].split("/")[0]
    if "/c/" in url:
        return "channel", url.split("/c/")[1].split("?")[0].split("/")[0]
    if "/user/" in url:
        return "channel", url.split("/user/")[1].split("?")[0].split("/")[0]
    if "/@" in url:
        return "channel", "@" + url.split("/@")[1].split("?")[0].split("/")[0]
        
    # Fallback: perhaps it's already an ID
    clean_id = url.split("?")[0].split("&")[0]
    return None, clean_id


def chunk_transcript(video, chunk_size=600):
    """
    Split transcripts into chunks suitable for RAG.
    Creates chunks of ~`chunk_size` characters, preserving sentences.
    """
    text = video.get("transcript", "")
    if not text:
        return []
        
    chunks = []
    # Simple regex to split roughly by sentences
    sentences = re.split(r'(?<=[.!?])\s+', text)
    
    current_chunk = ""
    chunk_index = 1
    
    for sentence in sentences:
        if len(current_chunk) + len(sentence) < chunk_size:
            current_chunk += sentence + " "
        else:
            if current_chunk.strip():
                chunks.append({
                    "video_id": video["id"],
                    "title": video["title"],
                    "channel": video.get("channel_title", ""),
                    "chunk_id": chunk_index,
                    "text": current_chunk.strip()
                })
                chunk_index += 1
            current_chunk = sentence + " "
            
    if current_chunk.strip():
        chunks.append({
            "video_id": video["id"],
            "title": video["title"],
            "channel": video.get("channel_title", ""),
            "chunk_id": chunk_index,
            "text": current_chunk.strip()
        })
        
    return chunks


def export_json(output_file, output_dict):
    """Export standard JSON."""
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output_dict, f, indent=2, ensure_ascii=False)


def export_jsonl(output_file, output_dict):
    """Export to JSONL (one JSON object per line per video)."""
    with open(output_file, "w", encoding="utf-8") as f:
        for video in output_dict.get("videos", []):
            f.write(json.dumps(video, ensure_ascii=False) + "\n")


def export_csv(output_file, output_dict):
    """Export videos to CSV."""
    videos = output_dict.get("videos", [])
    if not videos:
        return
        
    keys_to_write = ["id", "title", "description", "published_at", "channel_title", "tags",
                     "thumbnail_url", "duration", "view_count", "like_count", "comment_count", 
                     "url", "transcript_language", "transcript_error", "transcript"]
                     
    with open(output_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys_to_write, extrasaction='ignore')
        writer.writeheader()
        for video in videos:
            row = video.copy()
            if "tags" in row and isinstance(row["tags"], list):
                row["tags"] = ", ".join(row["tags"])
            if row.get("transcript"):
                # Standardize newline spaces for cleaner CSVs
                row["transcript"] = row["transcript"].replace("\n", " ").replace("\r", "")
            writer.writerow(row)


def export_parquet(output_file, output_dict):
    """Export to Parquet using Pandas."""
    try:
        import pandas as pd
        videos = output_dict.get("videos", [])
        if not videos:
            return
            
        df = pd.DataFrame(videos)
        
        # Convert any list types to string to avoid Parquet type nesting issues sometimes
        if "tags" in df.columns:
            df["tags"] = df["tags"].apply(lambda x: ", ".join(map(str, x)) if isinstance(x, list) else x)
            
        df.to_parquet(output_file, engine="pyarrow", index=False)
    except ImportError:
        print("\n⚠️ pandas and pyarrow are required for parquet export. Install them with: pip install pandas pyarrow")


def export_rag_jsonl(output_file, videos):
    """Exports a chunked RAG dataset to _rag.jsonl"""
    # Create the target file name nicely
    base_name = os.path.splitext(output_file)[0]
    rag_file = f"{base_name}_rag.jsonl"
    
    with open(rag_file, "w", encoding="utf-8") as f:
        for video in videos:
            if video.get("transcript"):
                chunks = chunk_transcript(video)
                for chunk in chunks:
                    f.write(json.dumps(chunk, ensure_ascii=False) + "\n")


def load_existing_progress(output_file, export_format="json"):
    """Load existing JSON file to resume from where we left off.
    
    We ALWAYS read from the internal `.json` tracker regardless of the chosen --format.
    """
    json_path = output_file if export_format == "json" else f"{os.path.splitext(output_file)[0]}.json"
    
    if os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            videos = data.get("videos", [])
            done = sum(1 for v in videos if v["transcript"] is not None)
            pending = len(videos) - done
            if len(videos) > 0:
                print(f"\n📂 Found existing progress: {done}/{len(videos)} transcripts successfully fetched.")
                print(f"   {pending} videos still need transcripts. Resuming...\n")
            return videos
        except json.JSONDecodeError:
            print(f"\n⚠️ WARNING: {json_path} is empty or corrupted. Starting fresh.")
            return []
    return []


def save_progress(output_file, source_target, videos, export_format="json"):
    """Save current progress to JSON, plus format-specific outputs.
    
    Note: RAG generation is NOT triggered here. It runs once after scraping
    finishes, inside main.py, to avoid rebuilding the file after every video.
    
    Args:
        output_file (str): The path to the requested output file.
        source_target (str): The ID of the targeted entity.
        videos (list): The video dictionaries.
        export_format (str): json, jsonl, csv, or parquet.
    """
    output = {
        "source_target": source_target,
        "total_videos": len(videos),
        "videos": videos
    }
    
    # 1. ALWAYS save a standard JSON copy as our state tracker
    json_path = output_file if export_format == "json" else f"{os.path.splitext(output_file)[0]}.json"
    export_json(json_path, output)
    
    # 2. Export to requested format
    if export_format == "jsonl":
        export_jsonl(output_file, output)
    elif export_format == "csv":
        export_csv(output_file, output)
    elif export_format == "parquet":
        export_parquet(output_file, output)
