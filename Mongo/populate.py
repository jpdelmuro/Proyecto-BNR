#populate from data
import csv
import requests

BASE_URL = "http://localhost:8000"

def main():
    with open("data.csv") as fd:
        users_csv = csv.DictReader(fd)
        for user in users_csv:
            del user["userID"]
            user["name"] = user["name"].split("/")
            x = requests.post(BASE_URL+"/users", json=user)
            if not x.ok:
                print(f"Failed to post the user {x} - {user}")

if __name__ == "__main__":
    main()
