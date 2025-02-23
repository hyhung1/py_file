import pandas as pd
import json
import os

def convert_xlsx_to_json(xlsx_file, json_file=None):
    """
    Read TikTok comments data from an Excel file and convert it to JSON.
    
    Args:
        xlsx_file (str): Path to the Excel file
        json_file (str, optional): Path to the output JSON file. 
                                   If None, will use the same name as xlsx_file but with .json extension
    
    Returns:
        dict: The data as a Python dictionary
    """
    if not os.path.exists(xlsx_file):
        raise FileNotFoundError(f"Excel file not found: {xlsx_file}")
    
    # Set default JSON filename if not provided
    if json_file is None:
        json_file = os.path.splitext(xlsx_file)[0] + '.json'
    
    # Read the Excel file
    print(f"Reading Excel file: {xlsx_file}")
    df = pd.read_excel(xlsx_file)
    
    # Convert column names to match the expected format
    column_mapping = {
        'Text': 'text',
        'Created At': 'createdAt',
        'Like Count': 'likeCount',
        'Reply Count': 'replyCount',
        'Is Author Liked': 'isAuthorLiked',
        'Username': 'username',
        'Display Name': 'displayName',
        'Bio': 'bio',
        'Avatar URL': 'avatarUrl'
    }
    
    # Rename columns if they exist
    for old_col, new_col in column_mapping.items():
        if old_col in df.columns:
            df = df.rename(columns={old_col: new_col})
    
    # Convert 'Yes'/'No' to boolean for isAuthorLiked if it's a string
    if 'isAuthorLiked' in df.columns and df['isAuthorLiked'].dtype == 'object':
        df['isAuthorLiked'] = df['isAuthorLiked'].map({'Yes': True, 'No': False})
    
    # Convert numeric columns to appropriate types
    if 'replyCount' in df.columns:
        df['replyCount'] = df['replyCount'].fillna(0).astype(int)
    
    if 'likeCount' in df.columns:
        df['likeCount'] = df['likeCount'].fillna(0).astype(int)
    
    # Convert DataFrame to list of dictionaries
    records = df.to_dict(orient='records')
    
    # Write to JSON file
    print(f"Writing data to JSON file: {json_file}")
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    
    print(f"Successfully converted {len(records)} records to JSON")
    return records

def process_comments_to_json(input_json_file):
    """
    Read media_path from JSON file, find Excel files in the comments subdirectory,
    and convert them to JSON files in the same comments directories.
    
    Args:
        input_json_file (str): Path to the input JSON file with media_path
    
    Returns:
        list: List of paths to the created JSON files
    """
    # Check if input JSON file exists
    if not os.path.exists(input_json_file):
        raise FileNotFoundError(f"Input JSON file not found: {input_json_file}")
    
    # Read the input JSON file
    print(f"Reading input JSON file: {input_json_file}")
    with open(input_json_file, 'r', encoding='utf-8') as f:
        input_data = json.load(f)
    
    json_files_created = []
    total_comments_converted = 0
    restaurants_processed = 0
    
    # Process each item in the JSON array
    for index, item in enumerate(input_data):
        try:
            # Get restaurant details
            restaurant_name = item.get('eat_name', 'Unknown')
            media_path = item.get('media_path')
            
            if not media_path:
                print(f"Item {index}: Media path not found for {restaurant_name}")
                continue
            
            # Construct the comments directory path
            comments_path = os.path.join(media_path, 'comments')
            
            if not os.path.exists(comments_path):
                print(f"Item {index}: Comments directory not found: {comments_path}")
                continue
            
            # Look for Excel files in the comments directory
            excel_files = [f for f in os.listdir(comments_path) if f.endswith('.xlsx')]
            
            if not excel_files:
                print(f"Item {index}: No Excel files found in {comments_path}")
                continue
            
            restaurant_comments_count = 0
            
            # Process each Excel file
            for excel_file in excel_files:
                excel_path = os.path.join(comments_path, excel_file)
                print(f"Processing Excel file: {excel_path}")
                
                try:
                    # Generate JSON filename based on Excel filename
                    json_filename = os.path.splitext(excel_file)[0] + '.json'
                    json_path = os.path.join(comments_path, json_filename)
                    
                    # Convert Excel to JSON format and save in the same comments directory
                    comments_data = convert_xlsx_to_json(excel_path, json_path)
                    
                    # Add metadata to track successful conversions
                    json_files_created.append(json_path)
                    restaurant_comments_count += len(comments_data)
                    total_comments_converted += len(comments_data)
                    
                    print(f"Created JSON file: {json_path} with {len(comments_data)} comments")
                    
                except Exception as e:
                    print(f"Error processing {excel_path}: {str(e)}")
            
            if restaurant_comments_count > 0:
                restaurants_processed += 1
                print(f"Converted {restaurant_comments_count} comments for {restaurant_name}")
        
        except Exception as e:
            print(f"Error processing item {index}: {str(e)}")
    
    # Print summary
    print("\n" + "="*50)
    print(f"SUMMARY:")
    print(f"Total restaurants processed: {restaurants_processed}")
    print(f"Total comments converted: {total_comments_converted}")
    print(f"Total JSON files created: {len(json_files_created)}")
    print("="*50)
    
    return json_files_created

# Example usage
if __name__ == "__main__":
    # Specify the input JSON file with media_path
    input_json_file = "./QUANAN/q10/quán_ngon_quận_10_upd_addurl.json"
    
    # Process all comments from all restaurants and save JSON files in their respective comments folders
    json_files = process_comments_to_json(input_json_file)
    
    # Print paths of created files
    if json_files:
        print("\nCreated JSON files:")
        for file_path in json_files:
            print(f"  - {file_path}")