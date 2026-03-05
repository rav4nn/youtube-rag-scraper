import time
import re
import urllib.request

def resolve_channel_handle(youtube, handle):
    """Resolve a YouTube handle like '@jameshoffmann' to a channel ID.
    
    Strategy:
        1. Scrape the channel page HTML for the canonical channelId.
        2. If that fails, fall back to YouTube Data API search.
    
    Args:
        youtube: An authenticated googleapiclient.discovery.build object.
        handle (str): The YouTube handle, with or without the '@'.
        
    Returns:
        str: The internal 'UC...' channel ID, or None if not found.
    """
    if handle.startswith("@"):
        handle = handle[1:]
        
    print(f"Resolving channel handle @{handle}...")
    
    # Method 1: Scrape the channel page HTML for the canonical channelId
    try:
        url = f"https://www.youtube.com/@{handle}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as response:
            html = response.read().decode("utf-8", errors="ignore")
        
        match = re.search(r'"channelId":"(UC[a-zA-Z0-9_-]+)"', html)
        if match:
            channel_id = match.group(1)
            print(f"  → Resolved via HTML scrape: {channel_id}")
            return channel_id
    except Exception as e:
        print(f"  ⚠️ HTML scrape failed ({e}), falling back to API search...")
    
    # Method 2: Fallback to YouTube Data API search
    try:
        request = youtube.search().list(
            part="snippet",
            type="channel",
            q=handle,
            maxResults=1
        )
        response = request.execute()
        
        items = response.get("items", [])
        if items:
            channel_id = items[0]["snippet"]["channelId"]
            print(f"  → Resolved via API search: {channel_id}")
            return channel_id
    except Exception as e:
        print(f"  ⚠️ API search failed: {e}")
        
    print(f"  ❌ Could not resolve handle @{handle}")
    return None


def get_all_video_ids(youtube, channel_id):
    """Fetch all video IDs from the channel's uploads playlist.
    
    Args:
        youtube: An authenticated googleapiclient.discovery.build object.
        channel_id: The YouTube Channel ID string.
        
    Returns:
        list: A complete list of Video IDs for the given channel.
    """
    if "youtube.com/channel/" in channel_id:
        channel_id = channel_id.split("youtube.com/channel/")[-1].split("?")[0].split("&")[0].split("/")[0]
        
    uploads_playlist_id = "UU" + channel_id[2:] if channel_id.startswith("UC") else channel_id
        
    video_ids = []
    next_page_token = None

    print(f"Fetching video IDs from channel (via uploads playlist): {uploads_playlist_id}...")

    while True:
        request = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=uploads_playlist_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()

        for item in response.get("items", []):
            if "contentDetails" in item and "videoId" in item["contentDetails"]:
                video_ids.append(item["contentDetails"]["videoId"])

        next_page_token = response.get("nextPageToken")
        print(f"  Fetched {len(video_ids)} video IDs so far...")

        if not next_page_token:
            break
        time.sleep(0.5)

    print(f"Total videos found: {len(video_ids)}")
    return video_ids


def get_all_playlist_video_ids(youtube, playlist_id):
    """Fetch all video IDs from a playlist using pagination.
    
    Args:
        youtube: An authenticated googleapiclient.discovery.build object.
        playlist_id: The YouTube Playlist ID string.
        
    Returns:
        list: A complete list of Video IDs for the given playlist.
    """
    if "youtube.com/show/" in playlist_id:
        playlist_id = playlist_id.split("youtube.com/show/")[-1].split("?")[0].split("&")[0].split("/")[0]
        
    if playlist_id.startswith("VLPL"):
        playlist_id = playlist_id[2:]

    match = re.search(r"[?&]list=([^&]+)", playlist_id)
    if match:
        playlist_id = match.group(1)
    else:
        playlist_id = re.split(r"[?&%]", playlist_id)[0]

    video_ids = []
    next_page_token = None

    print(f"Fetching video IDs from playlist: {playlist_id}...")

    while True:
        request = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()

        for item in response.get("items", []):
            if "contentDetails" in item and "videoId" in item["contentDetails"]:
                video_ids.append(item["contentDetails"]["videoId"])

        next_page_token = response.get("nextPageToken")
        print(f"  Fetched {len(video_ids)} video IDs so far...")

        if not next_page_token:
            break
        time.sleep(0.5)

    print(f"Total videos found in playlist: {len(video_ids)}")
    return video_ids


def get_video_details(youtube, video_ids):
    """Fetch metadata and statistics for all videos in batches of 50.
    
    Args:
        youtube: An authenticated googleapiclient.discovery.build object.
        video_ids (list): A list of YouTube video IDs.
        
    Returns:
        list: A list of dictionaries containing video metadata.
    """
    all_videos = []

    print("\nFetching video metadata and statistics...")

    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        request = youtube.videos().list(
            part="snippet,statistics,contentDetails",
            id=",".join(batch)
        )
        response = request.execute()

        for item in response.get("items", []):
            snippet = item.get("snippet", {})
            stats = item.get("statistics", {})
            content = item.get("contentDetails", {})

            video = {
                "id": item["id"],
                "title": snippet.get("title", ""),
                "description": snippet.get("description", ""),
                "published_at": snippet.get("publishedAt", ""),
                "channel_title": snippet.get("channelTitle", ""),
                "tags": snippet.get("tags", []),
                "thumbnail_url": snippet.get("thumbnails", {}).get("high", {}).get("url", ""),
                "duration": content.get("duration", ""),
                "view_count": int(stats.get("viewCount", 0)),
                "like_count": int(stats.get("likeCount", 0)),
                "comment_count": int(stats.get("commentCount", 0)),
                "url": f"https://www.youtube.com/watch?v={item['id']}",
                "transcript": None,
                "transcript_language": None,
                "transcript_error": None
            }
            all_videos.append(video)

        print(f"  Processed metadata for {min(i+50, len(video_ids))}/{len(video_ids)} videos...")
        time.sleep(0.3)

    return all_videos
