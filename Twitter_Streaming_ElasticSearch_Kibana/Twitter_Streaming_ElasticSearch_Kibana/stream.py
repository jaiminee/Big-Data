import json
import tweepy
import re
import string
import googlemaps

import socket


ACCESS_TOKEN = '154880343-G8qtn1qBIHo4QCTRgBPQhVUoi6mHDEyQRuRidCqP'
ACCESS_SECRET = 'WOEfXZjHiYRgIBOyU8KdHonpqV3qlEPI7u19xq1G9icuc'
CONSUMER_KEY = 'FRoBIsvKQq02TFovbrM2YeUh8'
CONSUMER_SECRET = '19d86AgB0uhQBmapnr5a8pP5SM2OqnpDVY5eqZ1OwgAx9kHsaQ'

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)


hashtag = '#guncontrolnow'

TCP_IP = 'localhost'
TCP_PORT = 9006

# create sockets
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("here")
conn, addr = s.accept()
print("connection accepted")


class MyStreamListener(tweepy.StreamListener):
    def on_status(self, status):
            noEmoticons = ''.join([x for x in status.text if x in string.printable])
            location = status.user.location
            gmaps = googlemaps.Client(key='AIzaSyB7yjs1Vvwe9S_VqvhuO3Zd2JSwONIFTb0')
            if isinstance(location,str):
                # Geocoding an address
                
                try:
                    #print(location)
                    geocode_result = gmaps.geocode(location)
                    #print(geocode_result)
                    #gotdata = dlist[1]
                    lat = geocode_result[0]["geometry"]["location"]["lat"]
                    lon = geocode_result[0]["geometry"]["location"]["lng"]
                    latLonResultWithTweet = str(lat) +"@"+ str(lon) +"#"+ noEmoticons +"\n"
                    print(latLonResultWithTweet)
                    print("\n")
                    print("\n")
                    
                    conn.send(latLonResultWithTweet.encode('utf-8'))
                except:
                    print("Data unavailable")
    
    def on_error(self, status_code):
        if status_code == 420:
            return False
        else:
            print(status_code)

myStream = tweepy.Stream(auth=auth, listener=MyStreamListener())

myStream.filter(track=[hashtag])
