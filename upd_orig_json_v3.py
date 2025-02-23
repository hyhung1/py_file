import json
import os
import pandas as pd

def extract_excel_data(xlsx_file):
    """Extract restaurant data from Excel file with usn_time as key"""
    try:
        if not os.path.exists(xlsx_file):
            print(f"Error: Excel file '{xlsx_file}' does not exist.")
            return {}
            
        df = pd.read_excel(xlsx_file)
        restaurant_data = {}
        
        for _, row in df.iterrows():
            usn_time = row.get('usn_time')
            if not usn_time:
                continue
                
            restaurant_data[usn_time] = {
                'eat_name': row.get('eat_name', ''),
                'eat_addr': row.get('eat_addr', ''),
                'open_time': row.get('open_time', ''),
                'menu': row.get('menu', '')
            }
                
        return restaurant_data
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return {}

def update_json_data(json_file, restaurant_data):
    """
    Update JSON file with restaurant details based on matching usn_time
    Only keep entries that have a match in the restaurant_data
    """
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        updated_count = 0
        removed_count = 0
        filtered_data = []
        
        # Only keep entries that have a matching usn_time in restaurant_data
        for entry in data:
            usn_time = entry.get('usn_time', '')
            
            if usn_time and usn_time in restaurant_data:
                # Update entry with restaurant data
                entry.update(restaurant_data[usn_time])
                filtered_data.append(entry)
                updated_count += 1
            else:
                removed_count += 1
        
        file_name, file_ext = os.path.splitext(json_file)
        new_file = f"{file_name}_upd{file_ext}"
        
        with open(new_file, 'w', encoding='utf-8') as f:
            json.dump(filtered_data, f, ensure_ascii=False, indent=4)
        
        print(f"Updated and kept {updated_count} entries, removed {removed_count} entries.")
        print(f"Total entries in original file: {len(data)}")
        print(f"Total entries in updated file: {len(filtered_data)}")
        print(f"Saved to {new_file}")
        return new_file
    except Exception as e:
        print(f"Error updating JSON file: {e}")
        return None

def main():
    json_file = "./QUANAN/q10/quán_ngon_quận_10.json"
    excel_file = "./QUANAN/q10/quán_ngon_quận_10_ver2.xlsx"
    
    if not os.path.exists(json_file):
        print(f"Error: JSON file '{json_file}' does not exist.")
        return
    
    if not os.path.exists(excel_file):
        print(f"Error: Excel file '{excel_file}' does not exist.")
        return
    
    restaurant_data = extract_excel_data(excel_file)
    
    if restaurant_data:
        update_json_data(json_file, restaurant_data)
    else:
        print("No restaurant data found in Excel file.")

if __name__ == "__main__":
    main()