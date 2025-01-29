import requests
import json
import pandas as pd
from sqlalchemy import create_engine

# postgresql connection data
postgres_user = ""
password = ""
host = ""
port = "5432" # default port
table = ""

# Replace with your GitHub token and repository details
GITHUB_TOKEN = ""
REPO_OWNER = "kubernetes"
REPO_NAME = "kubernetes"
URL = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/issues"
HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}
PARAMS = {
    "state": "closed",  # Fetch only closed issues
    "label": "kind/bug",
    "is":"issue",
    "per_page": 100,    # Number of issues per page (max 100)
    "page": 1           # Start with page 1
}

# Fetch issues
issues = []
while True:
    response = requests.get(URL, headers=HEADERS, params=PARAMS)
    if response.status_code != 200:
        print(f"Error: {response.status_code}")
        break

    data = response.json()
    if not data:
        break

    issues.extend(data)
    PARAMS["page"] += 1  # Move to the next page

def getvalOrNone(val, prop):
    if val is None:
        return None
    else:
        return val.get(prop)
# Flatten the issues
flattened_issues = []
for issue in issues:
    flattened = {
        "issue_id": issue.get("id"),
        "number": issue.get("number"),
        "title": issue.get("title"),
        "body": issue.get("body"),
        "state": issue.get("state"),
        "created_at": issue.get("created_at"),
        "updated_at": issue.get("updated_at"),
        "closed_at": issue.get("closed_at"),
        "repository_url": issue.get("repository_url"),
        "comments": issue.get("comments"),
        "pull_request_url": getvalOrNone(issue.get("pull_request", {}), "url"),
        "pull_request_html_url": getvalOrNone(issue.get("pull_request", {}), "html_url"),
        "user_login": getvalOrNone(issue.get("user", {}), "login"),
        "user_id": getvalOrNone(issue.get("user", {}), "id"),
        "user_type": getvalOrNone(issue.get("user", {}), "type"),
        "label_names": [getvalOrNone(label, "name") for label in issue.get("labels", [])],
        "label_colors": [getvalOrNone(label, "color") for label in issue.get("labels", [])],
        "assignee_login": getvalOrNone(issue.get("assignee", {}), "login"),
        "assignee_id": getvalOrNone(issue.get("assignee", {}), "id"),
        "assignee_type": getvalOrNone(issue.get("assignee", {}), "type"),
        "milestone_title": getvalOrNone(issue.get("milestone", {}), "title"),
        "milestone_number": getvalOrNone(issue.get("milestone", {}), "number"),
        "milestone_state": getvalOrNone(issue.get("milestone", {}), "state"),
        "reactions_plus_one": issue.get("reactions", {}).get("+1", 0),
        "reactions_minus_one": issue.get("reactions", {}).get("-1", 0),
        "reactions_laugh": issue.get("reactions", {}).get("laugh", 0),
        "reactions_hooray": issue.get("reactions", {}).get("hooray", 0),
        "reactions_confused": issue.get("reactions", {}).get("confused", 0),
        "reactions_heart": issue.get("reactions", {}).get("heart", 0),
        "reactions_rocket": issue.get("reactions", {}).get("rocket", 0),
        "reactions_eyes": issue.get("reactions", {}).get("eyes", 0),
        "sa": 0,  # Default value for classification
        "ap": 0,  # Default value for classification
        "dp": 0,  # Default value for classification
        "comment": ""  # Default value for comment
    }
    flattened_issues.append(flattened)

# Save flattened issues to a JSON file (optional)
with open("flattened_github_issues.json", "w") as f:
    json.dump(flattened_issues, f, indent=2)

print(f"Fetched and flattened {len(flattened_issues)} closed issues.")


# Load JSON
df = pd.read_json('flattened_github_issues.json')

# Clean null char (\x00) in all string type columns
def clean_null_characters(value):
    if isinstance(value, str):
        return value.replace('\x00', '')  # Remove o caractere nulo
    return value

df = df.applymap(clean_null_characters)

# Create connection with postgres
engine = create_engine(f'postgresql://{postgres_user}:{password}@{host}:{port}/{table}')

# insert data
try:
    df.to_sql('github_issues', engine, if_exists='replace', index=False)
    print("Dados inseridos com sucesso!")
except Exception as e:
    print("Erro ao inserir os dados:", e)
