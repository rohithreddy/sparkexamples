from flask import Flask
from flask import render_template
import pymongo
connection = pymongo.MongoClient('mongodb://localhost')
db = connection.tweetlist
tweets = db.tweets

app = Flask(__name__)

@app.route("/")
def hello():
    return "Hello World!"

@app.route("/tweetslist")
def tweettohtml():
    cursor = tweets.find().sort('timestamp_ms',pymongo.DESCENDING).limit(20)
    docs = []
    for doc in cursor:
        docs.append(doc)
    return render_template('tweet_index.html',cursor=docs)


if __name__ == "__main__":
    app.run(debug=True,host='0.0.0.0')
