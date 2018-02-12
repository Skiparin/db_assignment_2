from pymongo import MongoClient
import pymongo
from bson.regex import Regex
import re
client = MongoClient("mongodb://127.0.0.1:27017")
db = client.social_net
#tweet = db.tweets.find({"user": "_TheSpecialOne_"})[0]
tweet = db.tweets

'''
print("How many Twitter users are in the database?")
users = tweet.distinct("user")
print(len(users))

print("Which Twitter users link the most to other Twitter users? (Provide the top ten.)") 
filters = list(db.tweets.aggregate([{"$match":{"text":{'$regex':'\@'}}},{"$group":{"_id":"$user","text":{"$push":1}}}]))
for f in filters:
	f["text"] = sum(f["text"])
filters = sorted(filters, key=lambda r: r['text'], reverse=True)
print(filters[:10])


print("Which Twitter users link the most to other Twitter users? (Provide the top ten.)") 
pattern = re.compile("(?<=^|(?<=[^a-zA-Z0-9-_\\.]))@([A-Za-z]+[A-Za-z0-9_]+)")
regex = Regex.from_native(pattern)
filters = list(db.tweets.aggregate([{"$match": {"text": {"$regex": regex}}},
            {"$project": {"user": "$user", "texts": {"$split": ["$text", " "]}}},
            {"$unwind": "$texts"},{"$match": {"texts": {"$regex": regex}}}, 
            {"$group": {"_id": "$texts", "count": {"$sum": 1}}}]))
filters = sorted(filters, key=lambda r: r['count'], reverse=True)
print(filters[:10])


print("Who are the most active Twitter users (top ten)?")
filters = list(db.tweets.aggregate([{"$group": {"_id": "$user", "count": {"$sum": 1}}}]))
filters = sorted(filters, key=lambda r: r['count'], reverse=True)
print(filters[:10])
'''

print("Who are the five most grumpy (most negative tweets). (Provide five users for each group)")
filters = list(db.tweets.aggregate([{"$match": {"polarity": {"$eq": 0}}}, {"$group": {"_id": "$user", "count": {"$sum": 1}}}]))
filters = sorted(filters, key=lambda r: r['count'], reverse=True)
print(filters[:5])
print("Who are the five the most happy (most positive tweets)?")
filters = list(db.tweets.aggregate([{"$match": {"polarity": {"$eq": 4}}}, {"$group": {"_id": "$user", "count": {"$sum": 1}}}]))
filters = sorted(filters, key=lambda r: r['count'], reverse=True)
print(filters[:5])
#print("1287 error(s)s")

