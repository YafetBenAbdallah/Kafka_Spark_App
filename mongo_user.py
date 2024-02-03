import pymongo

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["mydatabase"]

# Create a users collection
messages_collection = db["messages"]

# Insert some sample data
user_data = [{}]

messages_collection.insert_many(user_data)
