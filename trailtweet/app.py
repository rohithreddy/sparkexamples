import tweepy
import json
import pymongo

connection = pymongo.MongoClient('mongodb://localhost')
db = connection.tweetlist
tweets = db.tweets



#import boto3
#dynamodb = boto3.resource('dynamodb')
#table = dynamodb.Table('tweetsf')
#print(table.creation_date_time)

consumer_key = "dVvU9OAtEMESNBRmqrHDHZY8x"
consumer_secret = "M01fR3LJhFCuqtoYBCdHyQ3vtldpFqCBQVN2ftiol5mgB99m77"
access_token = "4873715536-faeWNJFGtknR0mz5A9QQjHh8HtVEBb9JushyCPW"
access_token_secret = "7e7wcMJFulTmjTg0eDyUCLgpjUizH135KQh77OS48vlQk"


consumer_key1 = "juykXDcwpqmYXWwtBK4j7egO3"
consumer_secret1 = "cWBAFgk4DMK517p6uSJHqMLDgeULQucR6JgduN6REPSrt2AR4s"
access_token1 = "2853467773-jgslpGzjQEjROsv7E6TMSEz2vgwE5W4uEY1IxXS"
access_token_secret1 = "PYpWl1A3ucEZDeWyWxowiJdgbmyQHTD0eSzJ9u5opBlaJ"


auth = tweepy.OAuthHandler(consumer_key1,consumer_secret1)
auth.set_access_token(access_token1,access_token_secret1)
api = tweepy.API(auth)

class MyStreamListener(tweepy.StreamListener):
    def on_status(self, status):
        print(status.text)

    def on_data(self, data):
        decoded = json.loads(data)
	tweets.insert(decoded)
        #table.put_item(Item=decoded)
        print('@%s: %s' % (decoded['user']['screen_name'], decoded['text'].encode('ascii', 'ignore')))
        return True

myStreamListener = MyStreamListener()

myStream = tweepy.Stream(auth=api.auth,listener = myStreamListener)

try :
    myStream.filter(track = ['bangalore','spark','blackhole','Nuclear','IOT','freelance'])
except Exception as e:
    print("Exception" , type(e) , e)
