#library using this project
from googleapiclient.discovery import build
import googleapiclient.discovery
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

#google api key
api_key =< your google api Key >

api_service_name = "youtube"
api_version = "v3"

youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

#Channel data collection

def channel_info(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id       
    )
    response = request.execute()
    
    for i in response['items']:
        channel_data=dict (Channel_Name=i["snippet"]["title"],
                         Channel_Id=i["id"],
                         Subscription_Count=i["statistics"]["subscriberCount"],
                         Channel_Views=i["statistics"]["viewCount"],
                         Channel_Description=i["snippet"]["description"],
                         Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"],
                         Total_Videos=i["statistics"]["videoCount"],
                         Published=i["snippet"]["publishedAt"])
    return channel_data


#video ids
def video_Ids(channel_id):
    video_ids=[]
    response = youtube.channels().list(
                           part='contentDetails',
                           id=channel_id).execute()
    Playlist_Id =response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response_1=youtube.playlistItems().list(
                                     part='snippet',
                                     playlistId=Playlist_Id,
                                     maxResults=50,
                                     pageToken=next_page_token).execute()
        for i in range(len(response_1['items'])):
            video_ids.append(response_1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response_1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids


def get_video_data(Video_Ids):
    video_data=[]
    for video_id in Video_Ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()

        for item in response['items']:
            data=dict(Channel_Name=item["snippet"]["channelTitle"],
                        Channel_Id=item["snippet"]["channelId"],
                        Video_Name=item['snippet']['title'],
                        Video_Id=item['id'],
                        Tags=item['snippet'].get('tags'),
                        Video_Description=item['snippet'].get('description'),
                        PublishedAT=item['snippet']['publishedAt'],
                        Duration=item['contentDetails']['duration'],
                        Views_Count=item['statistics'].get('viewCount'),
                        Likes_Count=item['statistics'].get('likeCount'),
                        Favorite_Count=item['statistics'].get('favoriteCount'),
                        Comments_Count=item['statistics'].get('commentCount'),
                        Caption=item['contentDetails']['caption'],
                        Thumbnail=item['snippet']['thumbnails']['default']['url']
                        )
            video_data.append(data)
    return video_data


def get_comments(video_ids):
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
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId']
                        )
                Comment_data.append(data)
    except:
        pass
    return Comment_data

#connect Mongodb 

client = pymongo.MongoClient("your localhost")
mydb = client["Youtube_data"]

#one function to get channel data

def channel_details(channel_id):
    chnl_details=channel_info(channel_id)
    vi_ids=video_Ids(channel_id)
    vi_data=get_video_data(vi_ids)
    cmt_details=get_comments(vi_ids)
    
    coll=mydb["Channel_details"]
    coll.insert_one({"channel_information":chnl_details,
                     "video_information":vi_data,
                     "comment_information":cmt_details})
    
    return "upload completed successfully"

#SQL Create table and insert values

def channels_table():
    myconnection=psycopg2.connect(host=,
                              user=,
                              password=,
                              database=,
                              port=)

    cursor=myconnection.cursor()

    drop_query='''drop table if exists channels'''
    cursor.execute(drop_query)
    myconnection.commit()

    try: 
        create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                         Channel_Id varchar(80) primary key,
                                                         Subscription_Count bigint,
                                                         Channel_Views bigint,
                                                         Channel_Description text,
                                                         Playlist_Id varchar(80),
                                                         Total_Videos int,
                                                         Published timestamp
                                                        )'''
    
        cursor.execute(create_query)
        myconnection.commit()
    
    except:
        print("channels table already created")
    
    
    chnl_list=[]
    mydb=client["Youtube_data"]
    coll=mydb["Channel_details"]
    for chnl_data in coll.find({},{"_id":0,"channel_information":1}):
        chnl_list.append(chnl_data["channel_information"])
    df=pd.DataFrame(chnl_list) 
    
    
    for index,row in df.iterrows():

        insert_query='''insert into channels(Channel_Name,
                                                Channel_Id,
                                                Subscription_Count,
                                                Channel_Views,
                                                Channel_Description,
                                                Playlist_Id,
                                                Total_Videos,
                                                Published)

                                            values(%s,%s,%s,%s,%s,%s,%s,%s)'''

        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Subscription_Count'],
                row['Channel_Views'],
                row['Channel_Description'],
                row['Playlist_Id'],
                row['Total_Videos'],
                row['Published'])
            
        try:
            cursor.execute(insert_query,values)
            myconnection.commit()
        
        except:
            print("channels values are already inserted")
                
                
#video table
def videos_table():
    myconnection=psycopg2.connect(host=,
                                  user=,
                                  password=,
                                  database=,
                                  port=)

    cursor=myconnection.cursor()

    drop_query='''drop table if exists videos'''
    cursor.execute(drop_query)
    myconnection.commit()

    create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                     Channel_Id varchar(100),
                                                     Video_Name varchar(200),
                                                     Video_Id varchar (50) primary key,
                                                     Tags text,
                                                     Video_Description text,
                                                     PublishedAT timestamp,
                                                     Duration interval,
                                                     Views_Count bigint,
                                                     Likes_Count bigint,
                                                     Favorite_Count int,
                                                     Comments_Count int,
                                                     Caption varchar(50),
                                                     Thumbnail varchar(200)
                                                     )'''

    cursor.execute(create_query)
    myconnection.commit()

    vi_list=[]
    mydb=client["Youtube_data"]
    coll=mydb["Channel_details"]
    for vi_data in coll.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df1=pd.DataFrame(vi_list)


    for index,row in df1.iterrows():

            insert_query='''insert into videos(Channel_Name,
                                                Channel_Id,
                                                Video_Name,
                                                Video_Id,
                                                Tags,
                                                Video_Description,
                                                PublishedAT,
                                                Duration,
                                                Views_Count,
                                                Likes_Count,
                                                Favorite_Count,
                                                Comments_Count,
                                                Caption,
                                                Thumbnail
                                                )

                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

            values=(row['Channel_Name'],
                    row['Channel_Id'],
                    row['Video_Name'],
                    row['Video_Id'],
                    row['Tags'],
                    row['Video_Description'],
                    row['PublishedAT'],
                    row['Duration'],
                    row['Views_Count'],
                    row['Likes_Count'],
                    row['Favorite_Count'],
                    row['Comments_Count'],
                    row['Caption'],
                    row['Thumbnail'])

            cursor.execute(insert_query,values)
            myconnection.commit()

    
#comments table
def comments_table():
    myconnection=psycopg2.connect(host=,
                              user=,
                              password=,
                              database=,
                              port=)

    cursor=myconnection.cursor()

    drop_query='''drop table if exists comments'''
    cursor.execute(drop_query)
    myconnection.commit()


    create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                        Comment_Text text,
                                                        Comment_Author varchar(150),
                                                        Comment_Published timestamp,
                                                        Video_Id varchar(50)
                                                        )'''
    cursor.execute(create_query)
    myconnection.commit()

    com_list=[]
    mydb=client["Youtube_data"]
    coll=mydb["Channel_details"]
    for com_data in coll.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df2=pd.DataFrame(com_list)


    for index,row in df2.iterrows():

            insert_query='''insert into comments(Comment_Id,
                                                Comment_Text,
                                                Comment_Author,
                                                Comment_Published,
                                                Video_Id
                                                )

                                                 values(%s,%s,%s,%s,%s)'''

            values=(row['Comment_Id'],
                    row['Comment_Text'],
                    row['Comment_Author'],
                    row['Comment_Published'],
                    row['Video_Id'])

            cursor.execute(insert_query,values)
            myconnection.commit()



#convert dataframe

def show_channels_table():
    chnl_list=[]
    mydb=client["Youtube_data"]
    coll=mydb["Channel_details"]
    for chnl_data in coll.find({},{"_id":0,"channel_information":1}):
        chnl_list.append(chnl_data["channel_information"])
    df=st.dataframe(chnl_list)
    
    return df


def show_videos_table():
    vi_list=[]
    mydb=client["Youtube_data"]
    coll=mydb["Channel_details"]
    for vi_data in coll.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df1=st.dataframe(vi_list)
    
    return df1


def show_comments_table():
    com_list=[]
    mydb=client["Youtube_data"]
    coll=mydb["Channel_details"]
    for com_data in coll.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df2=st.dataframe(com_list)
    
    return df2

#streamlit page 

st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
st.header("Collections Of Youtube Channel Data")

channel_id=st.text_input("Enter the channel ID")

if st.button("collect data"):
    ch_ids=[]
    mydb=client["Youtube_data"]
    coll=mydb["Channel_details"]
    for ch_data in coll.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])
        
    if channel_id in ch_ids:
        st.success("Channel details of the given channel id already exists")
            
    else:
        insert=channel_details(channel_id)
        st.success(insert)

if st.button("view channel information"):
    if channel_id:
            Channel_info = channel_info(channel_id)
            if Channel_info:
                channel_information_dict ={"channel information":Channel_info}
                st.write(channel_information_dict)
            else:
                st.warning("Channel information not found for the given ID.")

    if channel_id:
        vi_ids=video_Ids(channel_id)
        if vi_ids:
            video_info = get_video_data(vi_ids)
            if video_info:
                video_information_dict = {"video information": video_info}
                st.write(video_information_dict)
                
            comments_info = get_comments(vi_ids)
            if comments_info:
                comments_information_dict = {"comments information": comments_info}
                st.write(comments_information_dict)
                
            else:
                st.warning("Comments information not found for the given channel ID.")
        else:
            st.warning("Video information not found for the given channel ID.")

if st.button("Migrate to sql"):
    Table=tables()
    st.success(Table)
    
show_table=st.radio("SELECT THE TABLE BELOW TO VIEW",("CHANNELS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channels_table()
    
elif show_table=="VIDEOS":
    show_videos_table()

elif  show_table=="COMMENTS":
    show_comments_table()


#SQL connection for querys
    
myconnection=psycopg2.connect(host=,
                          user=,
                          password=,
                          database=,
                          port=)

cursor=myconnection.cursor()

question=st.selectbox("select your questions",("1. What are the names of all the videos and their corresponding channels?",
                                               "2. Which channels have the most number of videos, and how many videos do they have?",
                                               "3. What are the top 10 most viewed videos and their respective channels?",
                                               "4. How many comments were made on each video, and what are their corresponding video names?",
                                               "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                                               "6. What is the total number of likes for each video, and what are their corresponding video names?",
                                               "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                                               "8. What are the names of all the channels that have published videos in the year 2022?",
                                               "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                               "10. Which videos have the highest number of comments, and what are their corresponding channel names?"))

#1
if question=="1. What are the names of all the videos and their corresponding channels?":
    query1='''select video_name as videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    myconnection.commit()
    t1=cursor.fetchall()
    df1=pd.DataFrame(t1,columns=["video name","channel name"])
    st.write(df1)

#2
elif question== "2. Which channels have the most number of videos, and how many videos do they have?":
    query2='''select channel_name as channelname,total_videos as no_videos from channels
              order by total_videos desc'''
    cursor.execute(query2)
    myconnection.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","No of videos"])
    st.write(df2)

#3
elif question== "3. What are the top 10 most viewed videos and their respective channels?":
    query3='''select views_count as views,channel_name as channelname,video_name as videoname from videos
               where views_count is not null order by views_count desc limit 10'''
    cursor.execute(query3)
    myconnection.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channel name","video name"])
    st.write(df3)

#4
elif question== "4. How many comments were made on each video, and what are their corresponding video names?":
    query4='''select comments_count as no_comments,video_name as videoname from videos where comments_count is not null'''
    cursor.execute(query4)
    myconnection.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["no of comments","video name"])
    st.write(df4)

#5
elif question== "5. Which videos have the highest number of likes, and what are their corresponding channel names?":
    query5='''select video_name as videoname, channel_name as channelname, likes_count as likescount 
              from videos where likes_count is not null order by likes_count desc'''
    cursor.execute(query5)
    myconnection.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["video name","channel name","likescount"])
    st.write(df5)

#6
elif question== "6. What is the total number of likes for each video, and what are their corresponding video names?":
    query6='''select likes_count as likescount, video_name as videoname from videos'''
    cursor.execute(query6)
    myconnection.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["likescount","video name"])
    st.write(df6)

#7
elif question== "7. What is the total number of views for each channel, and what are their corresponding channel names?":
    query7='''select channel_name as channelname, channel_views as totalviews from channels'''
    cursor.execute(query7)
    myconnection.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channel name","total views"])
    st.write(df7)

#8
if question== "8. What are the names of all the channels that have published videos in the year 2022?":
    query8='''select video_name as videoname, publishedat as video_publish,channel_name as channelname from videos
               where extract (year from publishedat)=2022'''
    cursor.execute(query8)
    myconnection.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["video name","video_publish","channel name"])
    st.write(df8)
 #9   
elif question== "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query9='''select channel_name as channelname, AVG(duration) as avg_duration from videos group by channel_name'''
    cursor.execute(query9)
    myconnection.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channel name","avg_duration"])

    T9=[]
    for index,row in df9.iterrows():
        chnl_title=row["channel name"]
        average_duration=row["avg_duration"]
        average_duration_str=str(average_duration)
        T9.append(dict(chnltitle=chnl_title,avgduration=average_duration_str))
    df=pd.DataFrame(T9)
    st.write(df)
 #10   
elif question== "10. Which videos have the highest number of comments, and what are their corresponding channel names?":
    query10='''select video_name as videoname, channel_name as channelname, comments_count as comments from videos
             where comments_count is not null order by comments_count desc'''
    cursor.execute(query10)
    myconnection.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["video name","channel name","comments"])
    st.write(df10)

#SQL connection close
cursor.close()
myconnection.close()
