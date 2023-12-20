from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

#API key connection

def Api_connect():
    Api_Id="AIzaSyCoxGfsW9tYK3NmeUI7wJeP5_j0-rvxZ_o"

    api_service_name="youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=Api_Id)

    return youtube

youtube=Api_connect()


#Get channels information
def get_channel_info(channel_id):
    request=youtube.channels().list(
                    part="snippet,ContentDetails,statistics",
                    id=channel_id
    )
    response=request.execute()

    for i in response['items']:
        data=dict(Channel_Name=i["snippet"]["title"],
                  Channel_Id = i["id"],
                  Subscription_Count= i["statistics"]["subscriberCount"],
                  Views = i["statistics"]["viewCount"],
                  Total_Videos = i["statistics"]["videoCount"],
                  Channel_Description = i["snippet"]["description"],
                  Playlist_Id = i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data

#Get video ids
def get_video_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids

# Get video information
def get_video_info(video_ids):        
        video_data=[]
        for video_id in video_ids:
                request = youtube.videos().list(
                part="snippet,ContentDetails,statistics",
                id= video_id
                )        
                                
                response = request.execute()
                        
                for item in response["items"]:
                                data=dict(Channel_Name=item['snippet']['channelTitle'],
                                        Channel_Id=item['snippet']['channelId'],
                                        Video_Id=item['id'],
                                        Title=item['snippet']['title'],
                                        Tags=item['snippet'].get('tags'),
                                        Thumbnail=item['snippet']['thumbnails']['default']['url'],
                                        Description = item['snippet'].get('description'),
                                        Published_Date = item['snippet']['publishedAt'],
                                        Duration = item['contentDetails']['duration'],
                                        Views = item['statistics'].get('viewCount'),
                                        Likes = item['statistics'].get('likeCount'), #get is used to avoid error if there is not data in the like section it will print as 'null'
                                        Comments = item['statistics'].get('commentCount'),
                                        Favorite_Count = item['statistics']['favoriteCount'],
                                        Definition = item['contentDetails']['definition'],
                                        Caption_Status = item['contentDetails']['caption']
                                        )
                                video_data.append(data)
        return video_data


#get comment information
def get_comment_info(video_ids):
        Comment_data=[]
        try:
                for video_id in video_ids:
                                request=youtube.commentThreads().list(
                                        part="snippet",
                                        videoId=video_id,
                                        maxResults=50
                                )
                                response=request.execute()

                                for item in response['items']:
                                        data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                        Comment_published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                                
                                        Comment_data.append(data)
        except:
                pass
        return Comment_data

#get playlist details
def get_playlist_details(channel_id):
    next_page_token=None
    All_data=[]
    while True:
        request=youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response=request.execute()

        for item in response['items']:
            data=dict(Playlist_Id=item['id'],
                    Title=item['snippet']['title'],                    
                    Channel_Id=item['snippet']['channelId'],
                    Channel_Name=item['snippet']['channelTitle'],
                    PublishedAt=item['snippet']['publishedAt'],
                    Video_Count=item['contentDetails']['itemCount'])  
            All_data.append(data)
            
        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break
    return All_data


#Upload to MongoDB
client=pymongo.MongoClient("mongodb://localhost:27017/")
db=client["Youtube_data"]


def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_video_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)

    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                      "video_information":vi_details,"comment_information":com_details})
    
    return "upload completed sucessfully"

#Table creation for channels,playlists, videos, comments
def channels_table():
    mydb = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="saravana",
                        database="youtube_data",
                        port="5432")
    cursor=mydb.cursor()

    drop_query='''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()
    
    create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                        Channel_Id varchar(80) primary key,
                                                        Subscription_Count bigint,
                                                        Views bigint,
                                                        Total_Videos int,
                                                        Channel_Description text,
                                                        Playlist_Id varchar(80))'''
    
    cursor.execute(create_query)
    mydb.commit()

    ch_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)

    for index,row in df.iterrows():
        insert_query = '''insert into channels(Channel_Name,
                                                Channel_Id,
                                                Subscription_Count,
                                                Views,
                                                Total_Videos,
                                                Channel_Description,
                                                Playlist_Id
    )
                                            
                                                values(%s,%s,%s,%s,%s,%s,%s)'''              
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Subscription_Count'],
                row['Views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_Id'])      
                    
                    
                        
        cursor.execute(insert_query,values)
        mydb.commit()


def playlists_table():


    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="saravana",
                        database="youtube_data",
                        port="5432")
    cursor=mydb.cursor()

    drop_query='''drop table if exists playlists'''
    cursor.execute(drop_query)
    mydb.commit()
    
    create_query='''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                        Title varchar(100),
                                                        Channel_Id varchar(100),
                                                        Channel_Name varchar(100),
                                                        PublishedAt timestamp,
                                                        Video_Count int)'''
    
    cursor.execute(create_query)
    mydb.commit()

    pl_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=pd.DataFrame(pl_list)

    for index,row in df1.iterrows():
        insert_query = '''insert into playlists(Playlist_Id,
                                            Title,
                                            Channel_Id,
                                            Channel_Name,
                                            PublishedAt,
                                            Video_Count
                                            )
                                        
                                            values(%s,%s,%s,%s,%s,%s)'''              
        values=(row['Playlist_Id'],
            row['Title'],
            row['Channel_Id'],
            row['Channel_Name'],
            row['PublishedAt'],
            row['Video_Count'])      
                
                
                    
                        
        cursor.execute(insert_query,values)
        mydb.commit()


def videos_table():
    mydb = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="saravana",
                        database="youtube_data",
                        port="5432")
    cursor=mydb.cursor()

    drop_query='''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()
    
    create_query='''create table if not exists videos(Channel_Name varchar(100),
                                            Channel_Id varchar(100),
                                            Video_Id varchar(100) primary key,
                                            Title varchar(150),
                                            Tags text,
                                            Thumbnail varchar(200),
                                            Description text,
                                            Published_Date timestamp,
                                            Duration interval,
                                            Views bigint,
                                            Likes bigint,
                                            Comments int,
                                            Favorite_Count int,
                                            Definition varchar(10),
                                            Caption_Status varchar(50)
                                                )'''

    cursor.execute(create_query)
    mydb.commit()

    vi_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=pd.DataFrame(vi_list)


    for index,row in df2.iterrows():
        insert_query = '''insert into videos(Channel_Name,
                                                Channel_Id,
                                                Video_Id,
                                                Title,
                                                Tags,
                                                Thumbnail,
                                                Description,
                                                Published_Date,
                                                Duration,
                                                Views,
                                                Likes,
                                                Comments,
                                                Favorite_Count,
                                                Definition,
                                                Caption_Status
                                            )
                                        
                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''              
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Video_Id'],
                row['Title'],
                row['Tags'],
                row['Thumbnail'],
                row['Description'],
                row['Published_Date'],
                row['Duration'],
                row['Views'],
                row['Likes'],
                row['Comments'],
                row['Favorite_Count'],
                row['Definition'],
                row['Caption_Status']        
                )      
                
                
                    
                        
        cursor.execute(insert_query,values)
        mydb.commit()


def comments_table():

    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="saravana",
                        database="youtube_data",
                        port="5432")
    cursor=mydb.cursor()

    drop_query='''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                        Video_Id varchar(50),
                                                        Comment_Text text,
                                                        Comment_Author varchar(150),
                                                        Comment_published timestamp
                                                        )'''

    cursor.execute(create_query)
    mydb.commit()

    com_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=pd.DataFrame(com_list)

    for index,row in df3.iterrows():
            insert_query = '''insert into comments(Comment_Id,
                                                    Video_Id,
                                                    Comment_Text,
                                                    Comment_Author,
                                                    Comment_published
                                                )
                                            
                                                values(%s,%s,%s,%s,%s)'''              
            
            values=(row['Comment_Id'],
                    row['Video_Id'],
                    row['Comment_Text'],
                    row['Comment_Author'],
                    row['Comment_published']
                    )
                    
                    
                        
                            
            cursor.execute(insert_query,values)
            mydb.commit()


def tables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()
    
    return "Tables Created Successfully"

def show_channels_table():
    ch_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)

    return df

def show_playlists_table():
    pl_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=st.dataframe(pl_list)

    return df1

def show_videos_table():
    vi_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=st.dataframe(vi_list)

    return df2

def show_comments_table():
    com_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=st.dataframe(com_list)

    return df3



#streamlit part

with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skill Take Away")
    st.caption('Python scripting')
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption(" Data Managment using MongoDB and SQL")
    
channel_id = st.text_input("Enter the Channel ID")

if st.button("Collect and Store data"):
    ch_ids = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])
    
    if channel_id in ch_ids:
        st.success("Channel Details of the given channel id already exists")
    else:
        insert=channel_details(channel_id)
        st.success(insert)

if st.button("Migrate to Sql"):
    Table=tables()
    st.success(Table)

show_table=st.radio("SELECT THE TABLE FOR VIEW",(":green[CHANNELS]",":orange[PLAYLISTS]",":red[VIDEOS]",":blue[COMMENTS]"))

if show_table == ":green[CHANNELS]":
    show_channels_table()
elif show_table == ":orange[PLAYLISTS]":
    show_playlists_table()
elif show_table ==":red[VIDEOS]":
    show_videos_table()
elif show_table == ":blue[COMMENTS]":
    show_comments_table()



#SQL Connection
mydb=psycopg2.connect(host="localhost",
                    user="postgres",
                    password="saravana",
                    database="youtube_data",
                    port="5432")
cursor=mydb.cursor()

question = st.selectbox("Please Select Your Question",
    ("1. All the videos and the Channel Name",
     "2. Channels with most number of videos",
     "3. 10 most viewed videos",
     "4. Comments in each video",
     "5. Videos with highest likes",
     "6. Likes of all videos",
     "7. Views of each channel",
     "8. Videos published in the year 2022",
     "9. Average duration of all videos in each channel",
     "10. Videos with highest number of comments"))