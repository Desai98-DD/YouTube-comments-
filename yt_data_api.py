# Scrape Or Download Comments Using Python Through The Youtube Data API
#pip install --upgrade google-auth-oauthlib google-auth-httplib2
#pip install --upgrade google-api-python-client
import pandas as pd
from pathlib import Path
#import time
from googleapiclient.discovery import build

api_key = "AIzaSyBdtiC1A1vI5c-0VPiYfOPuPURVicTIZ1Q" # Replace this dummy api key with your own.
youtube = build('youtube', 'v3', developerKey=api_key)
comment_file_location='output'

box = [['Id','Name', 'Comment', 'Time', 'Likes', 'Reply Count']]
cols = ['Id','Name', 'Comment', 'Time', 'Likes', 'Reply Count','Status']

def save_or_append_csv(filename,df):
    my_file = Path(filename)
    if my_file.is_file():
        df_old = pd.read_csv(filename,encoding='UTF-8')
        df_old.dropna(inplace=True)
        df_list = df.values.tolist()
        for i in df_list:
            if i[0] not in list(df_old['Id']):
                series_obj = pd.Series( i, index=cols )
                #print(df_old.dtypes)
                df_old=df_old._append(series_obj,ignore_index=True)
        df_old.to_csv(filename, index=False, header=True)
    else:
        df.to_csv(filename, index=False, header=True)
    
    return "Successful! Check the CSV file that you have just created."


def scrape_comments_with_replies(filename,ID):
    box=[]
    data2=None
    df=None
    data = youtube.commentThreads().list(part='snippet', videoId=ID, maxResults='100', textFormat="plainText").execute()
    #print(data)
    for i in data["items"]:
        id = i["snippet"]['topLevelComment']["id"]
        name = i["snippet"]['topLevelComment']["snippet"]["authorDisplayName"]
        comment = i["snippet"]['topLevelComment']["snippet"]["textDisplay"]
        published_at = i["snippet"]['topLevelComment']["snippet"]['publishedAt']
        likes = i["snippet"]['topLevelComment']["snippet"]['likeCount']
        replies = i["snippet"]['totalReplyCount']

        box.append([id,name, comment, published_at, likes, replies])

        totalReplyCount = i["snippet"]['totalReplyCount']

        if totalReplyCount > 0:

            parent = i["snippet"]['topLevelComment']["id"]

            data2 = youtube.comments().list(part='snippet', maxResults='100', parentId=parent,
                                            textFormat="plainText").execute()

            for i in data2["items"]:
                id = i["id"]
                name = i["snippet"]["authorDisplayName"]
                comment = i["snippet"]["textDisplay"]
                published_at = i["snippet"]['publishedAt']
                likes = i["snippet"]['likeCount']
                replies = ""

                box.append([id,name, comment, published_at, likes, replies])

    while ("nextPageToken" in data):

        data = youtube.commentThreads().list(part='snippet', videoId=ID, pageToken=data["nextPageToken"],
                                             maxResults='100', textFormat="plainText").execute()

        for i in data["items"]:
            id = i["snippet"]['topLevelComment']["snippet"]["id"]
            name = i["snippet"]['topLevelComment']["snippet"]["authorDisplayName"]
            comment = i["snippet"]['topLevelComment']["snippet"]["textDisplay"]
            published_at = i["snippet"]['topLevelComment']["snippet"]['publishedAt']
            likes = i["snippet"]['topLevelComment']["snippet"]['likeCount']
            replies = i["snippet"]['totalReplyCount']

            box.append([id,name, comment, published_at, likes, replies])

            totalReplyCount = i["snippet"]['totalReplyCount']

            if totalReplyCount > 0:

                parent = i["snippet"]['topLevelComment']["id"]

                data2 = youtube.comments().list(part='snippet', maxResults='100', parentId=parent,
                                                textFormat="plainText").execute()

                for i in data2["items"]:
                    id = i["snippet"]["id"]
                    name = i["snippet"]["authorDisplayName"]
                    comment = i["snippet"]["textDisplay"]
                    published_at = i["snippet"]['publishedAt']
                    likes = i["snippet"]['likeCount']
                    replies = ''

                    box.append([id,name, comment, published_at, likes, replies])

    df = pd.DataFrame({'Id': [i[0] for i in box],'Name': [i[1] for i in box], 'Comment': [i[2] for i in box], 'Time': [i[3] for i in box],
                       'Likes': [i[4] for i in box], 'Reply Count': [i[5] for i in box],'Status': [None for i in box]})
    save_or_append_csv(filename,df)
    

def main():
    while True:
        df = pd.read_csv("input.csv")
        df.dropna(inplace=True)
        df_list = df.values.tolist()
        for i in df_list:
            filename=comment_file_location+'/'+i[0]+'.csv'
            id=i[1]
            scrape_comments_with_replies(filename,id)
            
main()