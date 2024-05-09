from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

#API KEY Connection

def Api_connect():
    Api_ID = "AIzaSyAu9edXVS8i2lDAc43P1BxUg98xaSacPw4"
    Api_service_name = "youtube"
    Api_version = "v3"

    youtube = build(Api_service_name,Api_version,developerKey=Api_ID)

    return youtube

youtube = Api_connect()


#Get Channel Information
def get_Channelinfo(channel_id):
    request = youtube.channels().list(
                    part= "snippet,ContentDetails,statistics",
                    id=channel_id
    )
    response = request.execute()

    for i in response['items']:
        data = dict(channel_Name=i["snippet"]["title"],
                    channel_ID=i["id"],
                    subscribers=i["statistics"]["subscriberCount"],
                    views=i["statistics"]["viewCount"],
                    Total_Videos=i["statistics"]["videoCount"],
                    Channel_description=i["snippet"]["description"],
                    Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
        return data

#Get Video IDs
def get_VideoIds(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part="contentDetails").execute()
    playlist_Id=response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    next_pagetoken= None

    while True:
        response1=youtube.playlistItems().list(
                                        part="snippet",
                                        playlistId=playlist_Id,
                                        maxResults=50,
                                        pageToken=next_pagetoken).execute()
        for i in range(len(response1["items"])):
            video_ids.append(response1["items"][i]["snippet"]["resourceId"]['videoId'])
        next_pagetoken= response1.get("nextPageToken")
        
        if next_pagetoken is None:
            break
    return video_ids


#Get Video info
def get_videoInfo(vidId):
    video_data=[]
    for video_id in vidId:
        request=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response=request.execute()

        for item in response["items"]:
            data=dict(Channel_Name=item["snippet"]["channelTitle"],
                    channel_ID=item["snippet"]["channelId"],
                    video_Id=item["id"],
                    title=item["snippet"]["title"],
                    Tags=item["snippet"].get("tags"),
                    Thumbnail=item["snippet"]["thumbnails"]["default"]["url"],
                    Description=item["snippet"].get("description"),
                    Published_date=item["snippet"]["publishedAt"],
                    Duration=item["contentDetails"]["duration"],
                    Likes=item["statistics"].get("likeCount"),
                    Views=item["statistics"].get("viewCount"),
                    Comments=item["statistics"].get("commentCount"),
                    Favourite_count=item["statistics"]["favoriteCount"],
                    Definition=item["contentDetails"]["definition"],
                    Caption_status=item["contentDetails"]["caption"]
                    ) 
            video_data.append(data)
    return video_data

#Get Comment Info
def get_CommentInfo(VideoIDS):
    Comment_Data=[]
    try:
        for video_id in VideoIDS:
            request= youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response["items"]:
                data=dict(Comment_Id=item["snippet"]["topLevelComment"]["id"],
                        Video_id=item["snippet"]["topLevelComment"]["snippet"]["videoId"],
                        Comment=item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                        Comment_Author=item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                        Comment_Date=item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])
                Comment_Data.append(data)
    except:
        pass
    return Comment_Data

#Get Playlist IDs
def get_playlistDetails(channel_id):
    next_pagetoken=None
    alldata=[]

    while True:
        request=youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_pagetoken
        )
        response=request.execute()

        for item in response["items"]:
            data=dict(playlist_id=item["id"],
                Title=item["snippet"]["title"],
                Channel_id=item["snippet"]["channelId"],
                channel_Name=item["snippet"]["channelTitle"],
                PublishDate=item["snippet"]["publishedAt"],
                Video_count=item["contentDetails"]["itemCount"])
            alldata.append(data)
            
        next_pagetoken=response.get("nextPageToken")
        if next_pagetoken is None:
            break
    return alldata

#upload to MongoDB
client=pymongo.MongoClient("mongodb://localhost:27017")
db=client["Youtube_Data"]

def channel_Details(channel_id):
    chnl_details=get_Channelinfo(channel_id)
    playlist_details=get_playlistDetails(channel_id)
    vid_details=get_VideoIds(channel_id)
    vid_info=get_videoInfo(vid_details)
    comment_details=get_CommentInfo(vid_details)

    collect=db["channel_Details"]
    collect.insert_one({"channel_Information":chnl_details,"playlistinfo":playlist_details,
                    "video_info":vid_info,"comment_info":comment_details})

    return "uploaded Successfully"
           
# Table Creation for Channels,playlists,videos,comments
def channel_table(channel_Name_s):
    mydb=psycopg2.connect(host="localhost",
                    user="postgres",
                    password="046046",
                    database="youtube_data",
                    port="5432")
    cursor=mydb.cursor()

    create_query='''create table if not exists channels(channel_Name varchar(100),
                                                        channel_ID varchar(80) primary key,
                                                        subscribers bigint,
                                                        views bigint,
                                                        Total_Videos int,
                                                        Channel_description text,
                                                        Playlist_Id varchar(50))'''
    cursor.execute(create_query)
    mydb.commit()
        
    single_channel_detail=[]
    db=client["Youtube_Data"]
    collect=db["channel_Details"]
    for ch_data in collect.find({"channel_Information.channel_Name": channel_Name_s},{"_id":0}):
        single_channel_detail.append(ch_data["channel_Information"])
    df_single_channel_detail=pd.DataFrame(single_channel_detail)

    for index,row in df_single_channel_detail.iterrows():
        insert_query='''insert into channels(channel_Name,
                                            channel_ID,
                                            subscribers,
                                            views,
                                            Total_Videos,
                                            Channel_description,
                                            Playlist_Id)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['channel_Name'],
                row['channel_ID'],
                row["subscribers"],
                row["views"],
                row["Total_Videos"],
                row["Channel_description"],
                row["Playlist_Id"])
    
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            Note=f"provided channel name {channel_name_s} Already Exist"
            
            return Note
        


#playlist table
def playlist_table(channel_name_s):
    mydb=psycopg2.connect(host="localhost",
                user="postgres",
                password="046046",
                database="youtube_data",
                port="5432")
    cursor=mydb.cursor()

    create_query='''create table if not exists playlists(playlist_id varchar(100) primary key,
                                                    Title varchar(100),
                                                    channel_Id varchar(80),
                                                    channel_Name varchar(100) ,
                                                    publishedAt timestamp,
                                                    Video_count int)'''

    cursor.execute(create_query)
    mydb.commit()

    single_playlist_detail=[]
    db=client["Youtube_Data"]
    collect=db["channel_Details"]
    for ch_data in collect.find({"channel_Information.channel_Name": channel_name_s},{"_id":0}):
        single_playlist_detail.append(ch_data["playlistinfo"])
    df_single_playlist_detail=pd.DataFrame(single_playlist_detail[0])

    for index,row in df_single_playlist_detail.iterrows():
        insert_query='''insert into playlists(playlist_id,
                                                Title,
                                                channel_Id,
                                                channel_Name,
                                                publishedAt,
                                                Video_count)
                                                
                                                values(%s,%s,%s,%s,%s,%s)'''
        values=(row['playlist_id'],
                row['Title'],
                row["Channel_id"],
                row["channel_Name"],
                row["PublishDate"],
                row["Video_count"])
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            print("channels already inserted")       

#video table           
def video_table(channel_name_s):
    mydb=psycopg2.connect(host="localhost",
                user="postgres",
                password="046046",
                database="youtube_data",
                port="5432")
    cursor=mydb.cursor()

    create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                        channel_ID varchar(100),
                                                        video_Id varchar(100) primary key,
                                                        title varchar(100),
                                                        Tags text,
                                                        Thumbnail varchar(100),
                                                        Description text,
                                                        Published_date timestamp,
                                                        Duration interval,
                                                        Likes bigint,
                                                        Views bigint,
                                                        Comments int,
                                                        Favourite_count int,
                                                        Definition varchar(20),
                                                        Caption_status varchar(100))'''

    cursor.execute(create_query)
    mydb.commit()


    single_video_detail=[]
    db=client["Youtube_Data"]
    collect=db["channel_Details"]
    for ch_data in collect.find({"channel_Information.channel_Name": channel_name_s},{"_id":0}):
        single_video_detail.append(ch_data["video_info"])
    df_single_video_detail=pd.DataFrame(single_video_detail[0])


    for index,row in df_single_video_detail.iterrows():
        insert_query='''insert into videos(Channel_Name,
                                        channel_ID,
                                        video_Id,
                                        title,
                                        Tags,
                                        Thumbnail,
                                        Description,
                                        Published_date, 
                                        Duration,
                                        Likes,
                                        Views,
                                        Comments,
                                        Favourite_count,
                                        Definition,
                                        Caption_status)
                                                
                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                row['channel_ID'],
                row["video_Id"],
                row["title"],
                row["Tags"],
                row["Thumbnail"],
                row['Description'],
                row['Published_date'],
                row["Duration"],
                row["Likes"],
                row["Views"],
                row["Comments"],
                row["Favourite_count"],
                row["Definition"],
                row["Caption_status"])
        
        cursor.execute(insert_query,values)
        mydb.commit()



def comments_table(channel_name_s):
    mydb=psycopg2.connect(host="localhost",
                    user="postgres",
                    password="046046",
                    database="youtube_data",
                    port="5432")
    cursor=mydb.cursor()

    create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                    Video_id varchar(100),
                                                    Comment text,
                                                    Comment_Author varchar(150),
                                                    Comment_Date timestamp)'''

    cursor.execute(create_query)
    mydb.commit()

    single_comment_detail=[]
    db=client["Youtube_Data"]
    collect=db["channel_Details"]
    for ch_data in collect.find({"channel_Information.channel_Name": channel_name_s},{"_id":0}):
        single_comment_detail.append(ch_data["comment_info"])
    df_single_comment_detail=pd.DataFrame(single_comment_detail[0])

    for index,row in df_single_comment_detail.iterrows():
        insert_query='''insert into comments(Comment_Id,
                                            Video_id,
                                            Comment,
                                            Comment_Author,
                                            Comment_Date)
                                                
                                            values(%s,%s,%s,%s,%s)'''
        values=(row['Comment_Id'],
                row['Video_id'],
                row["Comment"],
                row["Comment_Author"],
                row["Comment_Date"])
                
        cursor.execute(insert_query,values)
        mydb.commit()

def tables(single_channel):
    Note=channel_table(single_channel)
    if Note:
        return Note

    else:
        playlist_table(single_channel)
        video_table(single_channel)
        comments_table(single_channel)
        return "table created"

def view_channeltable():
    ch_list=[]
    db=client["Youtube_Data"]
    collect=db["channel_Details"]
    for ch_data in collect.find({},{"_id":0,"channel_Information":1}):
        ch_list.append(ch_data["channel_Information"])
    df=st.dataframe(ch_list)
    return df

def view_playlisttable():
    pl_list=[]
    db=client["Youtube_Data"]
    collect=db["channel_Details"]
    for pl_data in collect.find({},{"_id":0,"playlistinfo":1}):
        for i in range(len(pl_data["playlistinfo"])):
            pl_list.append(pl_data["playlistinfo"][i])
    df1=st.dataframe(pl_list)
    return df1

def view_videotable():
    vi_list=[]
    db=client["Youtube_Data"]
    collect=db["channel_Details"]
    for vi_data in collect.find({},{"_id":0,"video_info":1}):
        for i in range(len(vi_data["video_info"])):
            vi_list.append(vi_data["video_info"][i])
    df2=st.dataframe(vi_list)
    return df2

def view_commentstable():
    cm_list=[]
    db=client["Youtube_Data"]
    collect=db["channel_Details"]
    for cm_data in collect.find({},{"_id":0,"comment_info":1}):
        for i in range(len(cm_data["comment_info"])):
            cm_list.append(cm_data["comment_info"][i])
    df3=st.dataframe(cm_list)
    return df3

# Streamlit
with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skill take Away")
    st.caption("Pyhton Scripting")
    st.caption("Data Collection")
    st.caption("MongoDb")
    st.caption("API integration")
    st.caption("Data management using mongodb nd SQL")

channel_Id=st.text_input("Enter Channel Id")

if st.button("collect and store data"):
    ch_ids=[]
    db=client["Youtube_Data"]
    collect=db["channel_Details"]
    for ch_data in collect.find({},{"_id":0,"channel_Information":1}):
        ch_ids.append(ch_data["channel_Information"]["channel_ID"])
    
    if channel_Id in ch_ids:
        st.success("given channel Id exist")
    else:
        insrt=channel_Details(channel_Id)
        st.success(insrt)

allchannels=[]
db=client["Youtube_Data"]
collect=db["channel_Details"]
for ch_data in collect.find({},{"_id":0,"channel_Information":1}):
    allchannels.append(ch_data["channel_Information"]["channel_Name"])
unique_channel=st.selectbox("select the channel",allchannels)

if st.button("Migrate to SQL"):
    Table=tables(unique_channel)
    st.success(Table)

show_table=st.radio("select the table to view",("channels","playlists","videos","comments"))

if show_table=="channels":
    view_channeltable()

elif show_table=="playlists":
    view_playlisttable()

elif show_table=="videos":
    view_videotable()

elif show_table=="comments":
    view_commentstable()


#SQL Connection
mydb=psycopg2.connect(host="localhost",
                user="postgres",
                password="046046",
                database="youtube_data",
                port="5432")
cursor=mydb.cursor()

question=st.selectbox("select question",("1.names of all the videos and their corresponding channels",
                                    "2.Which channels have the most number of videos, and how many videos do they have",
                                    "3.What are the top 10 most viewed videos and their respective channels",
                                    "4.How many comments were made on each video, and what are their corresponding video names",
                                    "5.Which videos have the highest number of likes, and what are their corresponding channel names",
                                    "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names",
                                    "7.What is the total number of views for each channel, and what are their corresponding channel names",
                                    "8.What are the names of all the channels that have published videos in the year 2022",
                                    "9.What is the average duration of all videos in each channel, and what are their corresponding channel names",
                                    "10.Which videos have the highest number of comments, and what are their corresponding channel names"))

if question=="1.names of all the videos and their corresponding channels":
    qury1='''select title as videos,channel_Name as channelname from videos'''
    cursor.execute(qury1)
    mydb.commit()
    table1=cursor.fetchall()
    df1=pd.DataFrame(table1,columns=["video title","channel name"])
    st.write(df1)

elif question=="2.Which channels have the most number of videos, and how many videos do they have":
    qury2='''select channel_Name as channelname,total_videos as no_of_videos from channels order by total_videos desc'''
    cursor.execute(qury2)
    mydb.commit()
    table2=cursor.fetchall()
    df2=pd.DataFrame(table2,columns=["channel name","total videos"])
    st.write(df2)
elif question=="3.What are the top 10 most viewed videos and their respective channels":
    qury3='''select views as views,channel_Name as channelname,title as videotitle from videos where views is not null order by views desc limit 10'''
    cursor.execute(qury3)
    mydb.commit()
    table3=cursor.fetchall()
    df3=pd.DataFrame(table3,columns=["views","channel name","video title"])
    st.write(df3)
elif question=="4.How many comments were made on each video, and what are their corresponding video names":
    qury4='''select comments as no_comments,title as videotitle from videos where comments is not null'''
    cursor.execute(qury4)
    mydb.commit()
    table4=cursor.fetchall()
    df4=pd.DataFrame(table4,columns=["no of comments","video title"])
    st.write(df4)
elif question=="5.Which videos have the highest number of likes, and what are their corresponding channel names":
    qury5='''select title as videotitle,channel_Name as channelname,Likes as likecount from videos where Likes is not null order by Likes desc'''
    cursor.execute(qury5)
    mydb.commit()
    table5=cursor.fetchall()
    df5=pd.DataFrame(table5,columns=["video title","channel name","like count"])
    st.write(df5)
elif question=="6.What is the total number of likes and dislikes for each video, and what are their corresponding video names":
    qury6='''select Likes as likecount,title as videotitle from videos'''
    cursor.execute(qury6)
    mydb.commit()
    table6=cursor.fetchall()
    df6=pd.DataFrame(table6,columns=["like count","video title"])
    st.write(df6)
elif question=="7.What is the total number of views for each channel, and what are their corresponding channel names":
    qury7='''select channel_Name as channelname, views as totalviews from channels'''
    cursor.execute(qury7)
    mydb.commit()
    table7=cursor.fetchall()
    df7=pd.DataFrame(table7,columns=["channel name","total views"])
    st.write(df7)
elif question=="8.What are the names of all the channels that have published videos in the year 2022":
    qury8='''select title as video_title,published_date as date, channel_Name as channelname from videos where extract(year from published_date)=2022'''
    cursor.execute(qury8)
    mydb.commit()
    table8=cursor.fetchall()
    df8=pd.DataFrame(table8,columns=["video title","published date","channel name"])
    st.write(df8)
elif question=="9.What is the average duration of all videos in each channel, and what are their corresponding channel names":
    qury9='''select channel_Name as channelname,AVG(Duration) as avg_duration from videos group by channel_Name'''
    cursor.execute(qury9)
    mydb.commit()
    table9=cursor.fetchall()
    df9=pd.DataFrame(table9,columns=["channelname","Avearageduration"])
    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["Avearageduration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    df0=pd.DataFrame(T9)
    st.write(df0)
elif question=="10.Which videos have the highest number of comments, and what are their corresponding channel names":
    qury10='''select title as videotitle,channel_Name as channelname, Comments as comments from videos where Comments is not null order by Comments desc'''
    cursor.execute(qury10)
    mydb.commit()
    table10=cursor.fetchall()
    df10=pd.DataFrame(table10,columns=["video title","channel name","comments"])
    st.write(df10)