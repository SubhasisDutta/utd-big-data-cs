#Will run only in python 3
from twython import Twython
from geopy.geocoders import Nominatim
from kafka import KafkaProducer
import sys
import time

TWITTER_APP_KEY = 'DdjoK3z4s96do5q6IHBXLTrgy' #supply the appropriate value
TWITTER_APP_KEY_SECRET = 'ht4OFivGab5DZvL8uBA3JeM7QZsFtWhkKsWXmeDafzdEH2DOJ3'
TWITTER_ACCESS_TOKEN = '1213618561-Ox3oiAfVC3wc8yQ6q1tNQ4C22FJ1OLe9FrxQkGu'
TWITTER_ACCESS_TOKEN_SECRET = '1O3aPyYANilxWm9BxhmcollHXdhccvxRquVVX6CGOmH1g'

t = Twython(app_key=TWITTER_APP_KEY,
            app_secret=TWITTER_APP_KEY_SECRET,
            oauth_token=TWITTER_ACCESS_TOKEN,
            oauth_token_secret=TWITTER_ACCESS_TOKEN_SECRET)

producer = KafkaProducer(bootstrap_servers='localhost:9092')
geolocator = Nominatim()

def produceTweets(tag):
    search = t.search(q=tag,  # **supply whatever query you want here**
                      count=100)
    tweets = search['statuses']
    counter = 0
    for tweet in tweets:
        if len(tweet['user']['location']) > 1:
            try:
                location = geolocator.geocode(tweet['user']['location'])
            except:
                location = None
            if not location is None:
                loc = (location.latitude, location.longitude)
                message = tweet['id_str'] + "#$#" + \
                          tweet['user']['location'] + "#$#" + \
                          str(loc) + "#$#" +\
                          tweet['text'] + "#$#" +\
                          tweet['created_at'] +"#$#" +\
                          tag
                print(message)
                producer.send('tweet_topic', bytes(message.encode('utf-8')))
                producer.flush()
                counter += 1
    print("Tweets with ",tag," found : ",counter)

if __name__ == '__main__':
    tags = sys.argv[1:]
    print("This Scrapper will get tweets of ",str(tags), " and push to kafka.")
    if len(tags) > 0:
        while True:
            for tag in tags:
                produceTweets("#"+tag)
    producer.close()