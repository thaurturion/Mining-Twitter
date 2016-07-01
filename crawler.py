#!/Users/fabian/anaconda/envs/py35/bin python

import tweepy
import os, sys, time, csv
import configparser

class CustomStreamListener(tweepy.StreamListener):
    
    #give the file a nice name


    date = time.strftime("%d-%m-%Y/")
    directory = "data/crawled-tweets/" + date
    
    if not os.path.exists(directory):
        os.makedirs(directory)

    fileCounter = len(os.listdir(directory))+1 
    file_number = (3-len(str(fileCounter)))*"0"+str(fileCounter)

    tweetsFilename = directory + str(file_number) + time.strftime("-tweets-%H-%M-%S.csv")
    
    #prepare the csv write so everything is written to a nice csv file
    csv.register_dialect(
        'mydialect',
        delimiter = ',',
        quotechar = '"',
        doublequote = True,
        skipinitialspace = True,
        lineterminator = '\n',
        quoting = csv.QUOTE_MINIMAL)
    tweetsFile = open(tweetsFilename, 'a', encoding="utf-8", newline='')
    tweetsWriter = csv.writer(tweetsFile, dialect='mydialect')

    tweetCount = 0
    limit = 1000000

    def on_status(self, status):
        flag = 0
        
        if status.coordinates != None:
            longitude = status.coordinates["coordinates"][0]
            latitude = status.coordinates["coordinates"][1]
            flag = 1      
        elif status.place.bounding_box.coordinates != None:
            xcoords = [row[0] for row in status.place.bounding_box.coordinates[0]]
            ycoords = [row[1] for row in status.place.bounding_box.coordinates[0]]

            longitude = round((1/len(xcoords))*sum(xcoords), 8)
            latitude = round((1/len(ycoords))*sum(ycoords), 8)
            flag = 2

        if flag == 1 or flag == 2:
            data = [status.user.id_str, status.timestamp_ms, status.text.replace('\n', ''), longitude, latitude, status.retweet_count, status.favorite_count, flag]
            #data = str(status.user.id_str) + ", " + str(status.timestamp_ms) + ", " +  str(status.text) + ", " + str(longitude) + ", " + str(latitude) + ", " + str(status.retweet_count) + ", " + str(status.favorite_count) + ", " + flag
            print(data)
            if data != []:        
                CustomStreamListener.tweetsWriter.writerow(data)
                self.tweetCount += 1

            if self.tweetCount % 1000 == 0:
                print(str(self.tweetCount))
            if self.tweetCount > self.limit:
            	self.tweetsFile.close()
            	return False
        

        #CustomStreamListener._ffile.write(data+'\n')


    def on_error(self, status_code):
        print(' {} Error: {}'.format(sys.stderr, status_code))
        return True

    def on_timeout(self):
        print('{} Timeout...'.format(sys.stderr))
        return True


def getStreamer():
    config = configparser.ConfigParser()
    config.read('../credentials.ini')
    
    consumer_key = config['Twitter']['consumer_key']
    consumer_secret = config['Twitter']['consumer_secret']
    access_key = config['Twitter']['access_key']
    access_secret = config['Twitter']['access_secret']
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)

    auth.set_access_token(access_key, access_secret)
    return tweepy.streaming.Stream(auth, CustomStreamListener())

def main():
    
    #create Stream listener object
    strapi = getStreamer()

    #Use sample API
    #http://isithackday.com/geoplanet-explorer/index.php?woeid=23424975
    #NE 60.854691, 1.76896
    #SW 49.16209, -13.41393
    #-9.23, 2.69, 60.85, 49.84 
    # bounding_box:[west_long south_lat east_long north_lat]
    strapi.filter(locations=[-13.41393,49.16209,1.76896,60.854691], stall_warnings=True, languages=["en"])   

if __name__ == "__main__":
    main()

