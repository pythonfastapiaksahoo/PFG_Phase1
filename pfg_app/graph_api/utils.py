from pfg_app import settings
import requests
# Function to get the folder ID for a specific folder name
def get_folder_id(folder_name, access_token):
    url = f"https://graph.microsoft.com/v1.0/users/{settings.graph_corporate_mail_id}/mailFolders"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        mail_folders = response.json().get('value', [])
        for folder in mail_folders:
            if folder['displayName'] == folder_name:
                return folder['id']
        print(f"Folder '{folder_name}' not found.")
        return None
    else:
        print(f"Error fetching folders: {response.status_code}, {response.text}")
        return None