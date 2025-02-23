import json
import os
import cv2
import requests
import re
from pprint import pprint
from apify_client import ApifyClient

def extract_frames(video_path, output_folder):
    """Extract frames from a video at 3-second intervals"""
    # Create a temporary folder for extraction
    import tempfile
    temp_dir = tempfile.mkdtemp()
    print(f"Created temporary directory for frame extraction: {temp_dir}")
    
    # Create the output folder if it doesn't exist
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Open the video file
    video_capture = cv2.VideoCapture(video_path)

    # Get the frames per second (fps) of the video
    fps = int(video_capture.get(cv2.CAP_PROP_FPS))
    total_frames = int(video_capture.get(cv2.CAP_PROP_FRAME_COUNT))
    duration = total_frames // fps
    
    # Calculate the interval (3 seconds)
    interval = 3
    
    # Track the frame paths
    frame_paths = []

    # Loop through the video at specified intervals
    for i in range(0, duration, interval):
        # Set the video position to the i-th second
        video_capture.set(cv2.CAP_PROP_POS_MSEC, i * 1000)
        success, frame = video_capture.read()
        if success:
            # Save the frame as an image file in temp directory
            frame_filename = f"frame_{i}.jpg"
            temp_frame_path = os.path.join(temp_dir, frame_filename)
            cv2.imwrite(temp_frame_path, frame)
            
            # Copy the frame to the output folder
            final_frame_path = os.path.join(output_folder, frame_filename)
            import shutil
            shutil.copy2(temp_frame_path, final_frame_path)
            
            frame_paths.append(final_frame_path)
        else:
            break

    # Release the video capture object
    video_capture.release()
    
    # Clean up the temporary directory
    import shutil
    shutil.rmtree(temp_dir)
    print(f"Removed temporary directory: {temp_dir}")
    
    return frame_paths
    
def download_mp4(url, output_path):
    """Download MP4 file from URL"""
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(output_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    file.write(chunk)
        print(f"Download completed: {output_path}")
        return True
    else:
        print(f"Failed to download file. Status code: {response.status_code}")
        return False

def sanitize_filename(name):
    """Convert a string to a valid filename"""
    # Replace spaces and special characters with underscores
    sanitized = re.sub(r'[^\w\-_\. ]', '_', name)
    # Replace multiple consecutive underscores with a single one
    sanitized = re.sub(r'_+', '_', sanitized)
    # Trim to avoid excessively long filenames
    if len(sanitized) > 50:
        sanitized = sanitized[:50]
    return sanitized

def process_json_file(api_key, json_file, project_folder):
    """Process JSON file to download videos and extract frames"""
    # Create project folder if it doesn't exist
    if not os.path.exists(project_folder):
        os.makedirs(project_folder)
    
    # Initialize Apify client
    client = ApifyClient(api_key)
    
    # Load JSON data
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            loaded_list = json.load(f)
    except Exception as e:
        print(f"Error loading JSON file: {e}")
        return
        
    # Add debugging to understand structure
    print(f"JSON structure example (first item):")
    if loaded_list:
        print(f"Keys in first item: {list(loaded_list[0].keys()) if isinstance(loaded_list[0], dict) else 'Not a dictionary'}")
    
    print(f"Loaded {len(loaded_list)} items from JSON file")
    
    # Process each item
    for idx, item in enumerate(loaded_list):
        # Get restaurant name from the item
        eat_name = item.get('eat_name', '')
        
        if not eat_name:
            print(f"Warning: No restaurant name found for item {idx+1}. Using index as name.")
            eat_name = f"restaurant_{idx+1}"
        
        # Sanitize folder name
        folder_name = sanitize_filename(eat_name)
        restaurant_folder = os.path.join(project_folder, folder_name)
        
        # Create folder for the restaurant
        if not os.path.exists(restaurant_folder):
            os.makedirs(restaurant_folder)
            
        # Create folders for videos, images, and comments at the same level
        vid_folder = os.path.join(restaurant_folder, "vid")
        img_folder = os.path.join(restaurant_folder, "img")
        comments_folder = os.path.join(restaurant_folder, "comments")
        
        # Create the folders if they don't exist
        for folder in [vid_folder, img_folder, comments_folder]:
            if not os.path.exists(folder):
                os.makedirs(folder)
        
        # Get TikTok URL from postPage field
        clip_url = item.get('postPage', '')
        
        if not clip_url:
            print(f"Warning: No URL found for {eat_name}. Skipping...")
            continue
        
        print(f"Processing {eat_name} (Item {idx+1}/{len(loaded_list)})")
        print(f"TikTok URL: {clip_url}")
        
        # Set media path relative to project folder with correct path separators
        relative_path = os.path.join(project_folder, folder_name).replace('\\', '/')
        item['media_path'] = relative_path
        
        # Download the video using Apify
        run_input = {
            "postURLs": [clip_url],
            "shouldDownloadVideos": True,
            "shouldDownloadCovers": False,
            "shouldDownloadSubtitles": False,
            "shouldDownloadSlideshowImages": False,
        }
        
        try:
            print(f"Calling Apify API to download video...")
            run = client.actor("S5h7zRLfKFEr8pdj7").call(run_input=run_input)
            print(f"Apify run completed, dataset ID: {run['defaultDatasetId']}")
            media_urls = None
            
            # Get download URLs from the dataset
            dataset_items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
            print(f"Dataset contains {len(dataset_items)} items")
            
            if dataset_items:
                first_item = dataset_items[0]
                print(f"Dataset item structure: {list(first_item.keys())}")
                
                # Look for media URLs in different possible field names
                possible_fields = ['mediaUrls', 'videoUrl', 'videoUrls', 'urls', 'video']
                
                for field in possible_fields:
                    if field in first_item:
                        media_urls = first_item[field]
                        print(f"Found media URLs in field '{field}': {type(media_urls)}")
                        break
                        
                if not media_urls and 'video' in first_item:
                    # Some APIs return a direct video object
                    media_urls = [first_item['video']]
            
            if not media_urls:
                print(f"No media URLs found for {eat_name}. Skipping...")
                continue
            
            # Update the item with download URL
            loaded_list[idx]['downloadUrl'] = media_urls
            
            # Handle different formats of mediaUrls (could be dict or list)
            video_url = ''
            if isinstance(media_urls, dict):
                video_url = media_urls.get('video', '')
            elif isinstance(media_urls, list):
                # If it's a list, try to find the video URL
                for url in media_urls:
                    if isinstance(url, str) and (url.endswith('.mp4') or 'video' in url):
                        video_url = url
                        break
                # If no specific video URL found, use the first one
                if not video_url and media_urls:
                    video_url = media_urls[0]
            
            if video_url:
                video_filename = f"{folder_name}.mp4"
                video_path = os.path.join(vid_folder, video_filename)
                
                success = download_mp4(video_url, video_path)
                
                if success:
                    # Extract frames to temporary directory and copy only image files to img folder
                    frame_paths = extract_frames(video_path, img_folder)
                    print(f"Extracted {len(frame_paths)} frames to {img_folder}")
                
        except Exception as e:
            print(f"Error processing {eat_name}: {e}")
    
    # Save updated JSON with download URLs and media paths
    output_json = json_file.replace('.json', '_addurl.json')
    
    with open(output_json, 'w', encoding='utf-8') as f:
        json.dump(loaded_list, f, ensure_ascii=False, indent=4)
    
    print(f"Updated JSON saved to {output_json}")
    print(f"Total items processed: {len(loaded_list)}")

def main():
    # Example usage
    api_key = "apify_api_85rFJrdop3zajntC7oFQy2DabXYYjH3hJMB5" # Replace with your Apify API key
    json_file = "./QUANAN/q10/quán_ngon_quận_10_upd.json"  # Replace with your JSON file path
    project_folder = "./q10_items"  # Updated project folder path
    
    process_json_file(api_key, json_file, project_folder)

if __name__ == "__main__":
    main()