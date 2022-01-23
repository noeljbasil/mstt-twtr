#!/usr/bin/env python
# coding: utf-8

# Importing Libraries
from   datetime import date
from   google_drive_downloader import GoogleDriveDownloader as gdd
from   nltk.corpus import stopwords
from   nltk.tokenize import word_tokenize
import pandas as pd
import pygsheets
import re
import snscrape.modules.twitter as sntwitter
import time
from   dateutil.relativedelta import relativedelta
import os
import requests
import math

print("\n================================================================")
print("PART 1: OVERALL ANALYSIS")
print("================================================================\n")

#program start time
start_time = time.time()

def get_all_tweets():
    """This function scraps last ~3200 tweets of STT"""
    
    # Creating list to append tweet data to
    tweets_list = []

    #setting proxies
    proxies = {'https': 'http://137.74.196.132:3128',}

    # Using TwitterSearchScraper to scrape data and append tweets to list
    for i,tweet in enumerate(sntwitter.TwitterProfileScraper(user='STOPTHETRAFFIK',proxies= proxies).get_items()):
        tweets_list.append([tweet.date, tweet.content, tweet.hashtags, tweet.likeCount, tweet.retweetCount,tweet.replyCount, tweet.quoteCount, tweet.retweetedTweet, tweet.quotedTweet, tweet.media, tweet.lang, tweet.url])

    # Creating a dataframe from the tweets list above
    tweets_df = pd.DataFrame(tweets_list, columns=['Datetime', 'Text', 'Hashtags', 'Like Count', 'Retweet Count','Reply Count','Quote Count', 'Retweeted', 'Quoted Tweet','Media','Language','URL'])
    tweets_df['Update Date'] = date.today().strftime("%d %b %Y")
    
    return tweets_df

print("\n================================")
print("Tweets collection started")
print("================================\n")

tweets_df = get_all_tweets()

print("\n================================")
print("Tweets collection completed")
print("================================\n")

def clean_up_url(df,column):
    """This function cleans up column values by replacing url with 1 and None with 0"""
    cleaned_column = df[column].apply(lambda x: 0 if x is None else 1)
    return cleaned_column

# cleaning up column values by replacing urls with 1 and None with 0
# but first saving the quoted tweet urls to extract user names later before getting rid
tweets_df['Quoted Tweet url'] = tweets_df['Quoted Tweet'] 

for column in ['Retweeted','Quoted Tweet','Media']:
    tweets_df[column] = clean_up_url(tweets_df,column)

# replacing None in hashtag column with empty list
tweets_df['Hashtags'] = tweets_df['Hashtags'].apply(lambda x: [] if x is None else x)
# calculating the number of hashtags in each tweet
tweets_df['Number of Hashtags'] = tweets_df['Hashtags'].apply(lambda x: len(x))

#setting tweet id to use as key for join
tweets_df = tweets_df.reset_index(drop=True).reset_index().rename(columns={"index": "Tweet id"})

# removing hashtags, @ mentions and other irrelevant characters/words
tweets_df['Cleaned Text'] = tweets_df['Text'].apply(lambda x: ' '.join(re.sub("(#[A-Za-z0-9_]+)|(@[A-Za-z0-9_]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)"," ",x).split()))

print("\n================================")
print("Tokenization starting.....")
print("================================\n")

# tokenizing words of cleaned text and removing stop words
word_list = []
stopword_list = set(stopwords.words('english'))
for text in tweets_df['Cleaned Text']:
    text_tokens = word_tokenize(text)
    tokens_without_sw = [word for word in text_tokens if not word in stopword_list]
    word_list.append(tokens_without_sw)

tweets_df['Word List'] = word_list

print("\n================================================================")
print("Tokenization completed. About to compute WC data frames...")
print("================================================================\n")

# Tableau backend for word cloud
tweets_df_hashtags_transformed = pd.DataFrame(pd.concat([tweets_df['Tweet id'], pd.DataFrame(tweets_df['Hashtags'].tolist())], axis=1).set_index('Tweet id').stack()).reset_index().drop(labels='level_1', axis=1).set_index('Tweet id').rename(columns={0: "Words"}).reset_index()
tweets_df_text_transformed     = pd.DataFrame(pd.concat([tweets_df['Tweet id'], pd.DataFrame(tweets_df['Word List'].tolist())], axis=1).set_index('Tweet id').stack()).reset_index().drop(labels='level_1', axis=1).set_index('Tweet id').rename(columns={0: "Words"}).reset_index()

#for each of the dataframes, extracting date, time, creating publish url for tweets and tweet type
#we also drop text column because it messes with parsing in tableau
def tweet_type(df):
    """This function lables the tweets based on the values of 'Retweeted' and 'Quoted Tweet' columns"""
    if df['Retweeted'] == 0 and df['Quoted Tweet'] == 0:
        return 'Original'
    elif df['Retweeted'] == 0 and df['Quoted Tweet'] == 1:
        return 'Quote'
    else:
        return 'Retweet'
    
# converting list value of hastags to comma separated string so as to use as tooltip in tableau
def list_to_string(df,column):
    """This function converts list values of a column into string"""
    string_list = []
    for list_value in df[column]:
        string=""
        i=0
        for value in list_value:
            if i==0:
                string = value
            else:
                string = string+','+value
            i+=1
        string_list.append(string)
    return string_list

#Finding word frequencies for WC dataframes
def word_frequency(df):
    df['Words'] = df['Words'].str.lower()
    frequency = df.groupby(['Words']).size().to_frame('Frequency').reset_index()
    merged    = df.merge(frequency,how='left',on='Words')
    merged['Frequency'].fillna(0, inplace=True)
    return merged


tweets_df['Date']           = tweets_df['Datetime'].apply(lambda x: x.date())
tweets_df['Tweet type']     = tweets_df.apply(lambda row: tweet_type(row), axis=1)
tweets_df['Hashtag String'] = list_to_string(tweets_df,'Hashtags')
tweets_df.drop(['Hashtags'], axis=1,inplace=True)

tweets_df_text_transformed     = word_frequency(tweets_df_text_transformed)
tweets_df_hashtags_transformed = word_frequency(tweets_df_hashtags_transformed)

print("\n================================================================")
print("WC data frames computed. Commencing upload to G Drive...")
print("================================================================\n")

#remember to share the google sheet file with the service account email id before running below code
#downloading the service account key from google drive
gdrive_id = os.environ['Google_drive_id']

gdd.download_file_from_google_drive(file_id=gdrive_id,
                                    dest_path='./secret_key.json',
                                    unzip=True)

#authenticating with google sheets with pygsheets
client = pygsheets.authorize(service_account_file='secret_key.json')

#open google sheet
gsheet_key = os.environ['Google_sheet_key']
google_sheet = client.open_by_key(gsheet_key)

#selecting specific sheets
all_tweets            = google_sheet.worksheet_by_title('All Tweets')
all_tweets_text_WC    = google_sheet.worksheet_by_title('All Tweets text word cloud')
all_tweets_hashtag_WC = google_sheet.worksheet_by_title('All Tweets hashtag word cloud')

#clearing existing values from the sheets
all_tweets.clear(start='A1', end=None, fields='*')
all_tweets_text_WC.clear(start='A1', end=None, fields='*')
all_tweets_hashtag_WC.clear(start='A1', end=None, fields='*')

#trimming files before uploading
tweets_df_trimmed                      = tweets_df.loc[:,['Tweet id', 'Tweet type','Date','Like Count', 'Retweet Count','Number of Hashtags','Media','URL','Hashtag String','Reply Count','Update Date']]
tweets_df_trimmed_rt_info              = tweets_df.loc[:,['Tweet id', 'Tweet type','Date','Like Count', 'Retweet Count','Number of Hashtags','Media','URL','Hashtag String','Reply Count','Update Date','Retweeted','Quoted Tweet','Text','Quoted Tweet url']]
tweets_df_text_transformed_trimmed     = tweets_df_text_transformed.loc[:,['Words', 'Frequency']]
tweets_df_hashtags_transformed_trimmed = tweets_df_hashtags_transformed.loc[:,['Words', 'Frequency']]

#writing dataframes into the sheets
all_tweets.set_dataframe(tweets_df_trimmed, start=(1,1))
all_tweets_text_WC.set_dataframe(tweets_df_text_transformed_trimmed, start=(1,1))
all_tweets_hashtag_WC.set_dataframe(tweets_df_hashtags_transformed_trimmed, start=(1,1))


print("\n================================================================")
print("PART 2: RETWEET ANALYSIS")
print("================================================================\n")

#Fetching the latest uploaded all tweets data

# This block of code is to pull display names of retweeted accounts. 
# We are using selenium because Snscrape is not working due to ip ban for display name scrapping and display names are blocked when scrapped using Beautiful soup

#subsetting for retweets and quoted tweets
retweets_df = tweets_df_trimmed_rt_info.loc[tweets_df['Retweeted']==1,:]
quotes_df   = tweets_df_trimmed_rt_info.loc[tweets_df['Retweeted']==0,:].loc[tweets_df['Quoted Tweet']==1,:]

#extracting the handles of tweets STT has retweeted or quoted tweets
retweets_df.loc[:,'Retweet Handle'] = retweets_df.loc[:,'Text'].apply(lambda x: re.search('RT @(.+?):', x).group(1))
quotes_df.loc[:,'Retweet Handle'] = quotes_df.loc[:,'Quoted Tweet url'].apply(lambda x: re.search('https://twitter.com/(.+?)/', str(x)).group(1))

#Creating dataframe for both retweets and quoted tweets
retweets_quotes_df = pd.concat([retweets_df, quotes_df], axis=0)

#getting unique users to remove redundant api calls to get user details
unique_retweet_ids = list(set(retweets_df.loc[:,'Retweet Handle'])) + list(set(quotes_df.loc[:,'Retweet Handle'])) 

print("\n================================================================")
print("Computed unique user handles. Fetching user info....")
print("================================================================\n")

bearer_token = os.environ['Bearer_token'] #get bearer token from enviromental variables. Update with the one from your api keys

def create_url(user_names_list, user_fields):
    # Specify the usernames that you want to lookup below
    # You can enter up to 100 comma-separated values.
    user_names = ','.join(user_names_list) if len(user_names_list)>1 else user_names_list[0]
    
    usernames = f"usernames={user_names}"
    url = "https://api.twitter.com/2/users/by?{}&{}".format(usernames, user_fields)
    print(url)
    return url


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """
    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2UserLookupPython"
    return r


def connect_to_endpoint(url):
    response = requests.request("GET", url, auth=bearer_oauth,)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response.json()

def get_user_info(list_of_user_names):
    
    usernames   = []
    followers   = []
    location    = []
    user_handle = []
    
    user_fields  = "user.fields=name,location,public_metrics"
    url = create_url(list_of_user_names,user_fields)
    json_response = connect_to_endpoint(url)
   
    for user in json_response['data']: #for valid users whose data is returned
        try:
            usernames.append(user['name'])
        except:
            usernames.append(user['username'])
        try:
            location.append(user['location'])
        except:
            location.append("")
        try:
            followers.append(user['public_metrics']['following_count'])
        except:
            followers.append(0)
        user_handle.append(user['username'])
    if 'errors' in list(json_response.keys()):
        for user in json_response['errors']: #for invalid users
            usernames.append(user["value"])
            location.append("")
            followers.append(0)
            user_handle.append(user["value"])
    return usernames,followers,location,user_handle

usernames   = []
followers   = []
location    = []
user_handle = []

#the following for loop partitions the user id list into sets of 100 and calls the scrapping function on each of them
#twitter api currently has a limit of 100 users per call
for iteration in range(math.ceil(len(unique_retweet_ids)/100)):
    if iteration == 0:
        sa_i = 0
        so_i = math.floor(len(unique_retweet_ids)/math.ceil(len(unique_retweet_ids)/100))
    elif iteration<(math.ceil(len(unique_retweet_ids)/100)-1):
        temp = so_i
        so_i = so_i + 100
        sa_i = temp
    else:
        sa_i = so_i
        so_i = len(unique_retweet_ids)+1
    usernames_temp,followers_temp,location_temp,user_handle_temp = get_user_info(unique_retweet_ids[sa_i:so_i])
    usernames   = usernames + usernames_temp
    followers   = followers + followers_temp
    location    = location + location_temp
    user_handle = user_handle + user_handle_temp  
    print("\n======================================================================================================")
    print(f"Iteration {iteration+1}/{math.ceil(len(unique_retweet_ids)/100)} completed.")
    print("======================================================================================================\n")

#consolidating as dataframe to join back with main retweet dataset
retweet_user_df = pd.DataFrame({'Retweet Handle':user_handle,'User Name':usernames, 'Followers':followers, 'Location':location})

#left join to retweet dataset
retweets_df_final=retweets_quotes_df.merge(retweet_user_df,how='left',on='Retweet Handle')

print("\n================================================================")
print("Commencing upload to G Drive.")
print("================================================================\n")

#selecting specific sheets
retweets              = google_sheet.worksheet_by_title('Retweets')

#clearing existing values from the sheets
retweets.clear(start='A1', end=None, fields='*')

#trimming files before uploading.
retweets_df_final_trimmed = retweets_df_final[['Date','Followers','User Name','Retweet Handle', 'Tweet id','Location','Update Date']]

#writing dataframes into the sheets
retweets.set_dataframe(retweets_df_final_trimmed, start=(1,1))




print("\n================================================================")
print("PART 3: #humantrafficking ANALYSIS")
print("================================================================\n")


def get_latest_HT_tweets():
    """This function scraps tweets with #humantrafficking for the last 6 months"""
    
    # creating list to append tweet data to
    hashtag_tweets_list = []

    # using TwitterHashtagScraper to scrape tweets for a specific hashtag from last 6 months and append it to list
    for i,tweet in enumerate(sntwitter.TwitterHashtagScraper(f'#humantrafficking since:{(date.today() - relativedelta(months=6)).strftime("%Y-%m-%d")}').get_items()):
        hashtag_tweets_list.append([tweet.date, tweet.content, tweet.hashtags, tweet.likeCount, tweet.retweetCount,tweet.replyCount, tweet.quoteCount, tweet.retweetedTweet, tweet.quotedTweet, tweet.media, tweet.lang, tweet.url])

    # creating a dataframe from the tweets list above
    hashtags_tweets_df = pd.DataFrame(hashtag_tweets_list, columns=['Datetime', 'Text', 'Hashtags', 'Like Count', 'Retweet Count','Reply Count','Quote Count', 'Retweeted', 'Quoted Tweet','Media','Language','URL'])

    # subsetting for english tweets
    hashtags_tweets_df = hashtags_tweets_df[hashtags_tweets_df['Language']=='en']

    # adding todays date for reference
    hashtags_tweets_df['Update Date'] = date.today().strftime("%d %b %Y")
    
    return hashtags_tweets_df

print("\n================================")
print("Tweets collection started")
print("================================\n")

hashtags_tweets_df = get_latest_HT_tweets()

print("\n================================")
print("Tweets collection completed")
print("================================\n")


# cleaning up column values by replacing url with 1 and None with 0
for column in ['Retweeted','Quoted Tweet','Media']:
    hashtags_tweets_df[column] = clean_up_url(hashtags_tweets_df,column)

# removing rows without any hastags i.e no space between hashtags in tweets which means they are not recognized
hashtags_tweets_df   = hashtags_tweets_df[hashtags_tweets_df.applymap(lambda x: x is not None)['Hashtags']]

# calculating the number of hashtags in each tweet
hashtags_tweets_df['Number of Hashtags'] = hashtags_tweets_df['Hashtags'].apply(lambda x: len(x))

# subsetting for only popular tweets (100 likes or greater. 100 is an arbitrary number can be modified based on intuition/data)
# we also create a tweet id so as to identify the tweet words/hashtags are associated with for future analysis
top_hastags_tweets_df = hashtags_tweets_df[hashtags_tweets_df['Like Count']>=100].reset_index(drop=True).reset_index().rename(columns={"index": "Tweet id"})

# removing hashtags, @ mentions and other irrelevant characters/words
top_hastags_tweets_df['Cleaned Text'] = top_hastags_tweets_df['Text'].apply(lambda x: ' '.join(re.sub("(#[A-Za-z0-9_]+)|(@[A-Za-z0-9_]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)"," ",x).split()))

print("\n================================")
print("Tokenization starting.....")
print("================================\n")

# tokenizing words of cleaned text and removing stop words
word_list = []
stopword_list = set(stopwords.words('english'))

for text in top_hastags_tweets_df['Cleaned Text']:
    text_tokens = word_tokenize(text)
    tokens_without_sw = [word for word in text_tokens if not word in stopword_list]
    word_list.append(tokens_without_sw)

top_hastags_tweets_df['Word List'] = word_list

print("\n================================================================")
print("Tokenization completed. About to compute WC data frames...")
print("================================================================\n")

# Tableau backend for word cloud
HT_hashtags_transformed = pd.DataFrame(pd.concat([top_hastags_tweets_df['Tweet id'], pd.DataFrame(top_hastags_tweets_df['Hashtags'].tolist())], axis=1).set_index('Tweet id').stack()).reset_index().drop(labels='level_1', axis=1).set_index('Tweet id').rename(columns={0: "Words"}).reset_index()
HT_text_transformed     = pd.DataFrame(pd.concat([top_hastags_tweets_df['Tweet id'], pd.DataFrame(top_hastags_tweets_df['Word List'].tolist())], axis=1).set_index('Tweet id').stack()).reset_index().drop(labels='level_1', axis=1).set_index('Tweet id').rename(columns={0: "Words"}).reset_index()

#for each of the dataframes, extracting date and tweet type
# converting list value of hastags to comma separated string so as to use as tooltip in tableau
#Finding word frequencies for WC dataframes
   
hashtags_tweets_df['Date']           = hashtags_tweets_df['Datetime'].apply(lambda x: x.date())
hashtags_tweets_df['Tweet type']     = hashtags_tweets_df.apply(lambda row: tweet_type(row), axis=1)
hashtags_tweets_df['Hashtag String'] = list_to_string(hashtags_tweets_df,'Hashtags')
hashtags_tweets_df.drop(['Hashtags'], axis=1,inplace=True)

HT_text_transformed                  = word_frequency(HT_text_transformed)
HT_hashtags_transformed              = word_frequency(HT_hashtags_transformed)

print("\n================================================================")
print("WC data frames computed. Commencing upload to G Drive")
print("================================================================\n")

#selecting specific sheets
HT_tweets             = google_sheet.worksheet_by_title('All HT tweets')
HT_text_WC            = google_sheet.worksheet_by_title('HT text word cloud')
HT_hashtag_WC         = google_sheet.worksheet_by_title('HT hashtag word cloud')


#clearing existing values from the sheets
HT_tweets.clear(start='A1', end=None, fields='*')
HT_text_WC.clear(start='A1', end=None, fields='*')
HT_hashtag_WC.clear(start='A1', end=None, fields='*')

#trimming files before uploading
hashtags_tweets_df_trimmed             = hashtags_tweets_df[['Date','Tweet type','Like Count', 'Retweet Count','Number of Hashtags','Media','Update Date','URL','Hashtag String']]
HT_text_transformed_trimmed            = HT_text_transformed[['Words', 'Frequency']]
HT_hashtags_transformed_trimmed        = HT_hashtags_transformed[['Words', 'Frequency']]

#writing dataframes into the sheets
HT_tweets.set_dataframe(hashtags_tweets_df_trimmed, start=(1,1))
HT_text_WC.set_dataframe(HT_text_transformed_trimmed, start=(1,1))
HT_hashtag_WC.set_dataframe(HT_hashtags_transformed_trimmed, start=(1,1))

#program end time
print(f"Program ran for {round((time.time() - start_time),2)} seconds.")