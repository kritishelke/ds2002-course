import os
from urllib.parse import quote_plus
from pymongo import MongoClient

def main():
    url = os.getenv("MONGODB_ATLAS_URL")
    user = os.getenv("MONGODB_ATLAS_USER")
    pwd = os.getenv("MONGODB_ATLAS_PWD")

    if not url or not user or not pwd:
        print("Missing one or more MongoDB Atlas environment variables.")
        return

    safe_user = quote_plus(user)
    safe_pwd = quote_plus(pwd)

    full_uri = "mongodb+srv://{0}:{1}@cluster0.ptbx0rc.mongodb.net/?retryWrites=true&w=majority".format(
        safe_user, safe_pwd
    )

    client = MongoClient(full_uri, serverSelectionTimeoutMS=5000)

    db = client["bookstore"]
    authors = db["authors"]

    total = authors.count_documents({})

    print("Bookstore Author Report")
    print("-----------------------")
    print("Total authors:", total)
    print()

    for author in authors.find({}, {"_id": 0, "name": 1, "nationality": 1}).sort("name", 1):
        print(author["name"] + " - " + author["nationality"])

    client.close()

if __name__ == "__main__":
    main()
