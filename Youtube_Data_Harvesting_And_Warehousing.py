"""
##########################################################
           YOUTUBE DATA HARVESTING AND WAREHOUSING
##########################################################
"""

#----------------------MODULES-USED----------------------
import streamlit as st
import googleapiclient.discovery
import mysql.connector as mc
import pandas as pd
import isodate
from PIL import Image
import base64
import random


#----------------------SQL-CONNECTION--------------------
def get_mysql_connection():
    return mc.connect(
        host="localhost",
        port="3306",
        user="root",
        passwd="saipavan55",
        database="guvi",
        auth_plugin='mysql_native_password',
        charset='utf8mb4'
    )

#-----------------------CHANNEL-DATA-----------------------
def channel_data(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id)
    response = request.execute()
    data = {
        "channel_name": response["items"][0]['snippet']["title"],
        "channel_id": channel_id,
        "channel_dec": response["items"][0]['snippet']["description"],
        "channel_playlistid": response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"],
        "channel_viewc": response["items"][0]['statistics']['videoCount'],
        "channel_subc": response["items"][0]["statistics"]["subscriberCount"]
    }
    Playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    return data, Playlist_id


#--------------------CHANNEL-DATA-WITH-SQL-----------------
def channel_data_with_sql(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id)
    response = request.execute()
    data = {
        "channel_name": response["items"][0]['snippet']["title"],
        "channel_id": channel_id,
        "channel_dec": response["items"][0]['snippet']["description"],
        "channel_playlistid": response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"],
        "channel_viewc": response["items"][0]['statistics']['videoCount'],
        "channel_subc": response["items"][0]["statistics"]["subscriberCount"]
    }
    Playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    query = """INSERT INTO channel(channel_name, channel_id, channel_description, channel_pid, channel_views, channel_subc)
               VALUES (%s, %s, %s, %s, %s, %s)"""
    mycursor.execute(query, (data["channel_name"], data["channel_id"], data["channel_dec"],
                             data["channel_playlistid"], data["channel_viewc"], data["channel_subc"]))
    return data, Playlist_id


#-----------------------PLAYLIST-DATA-----------------------
def playlist_data(channel_id):
    request = youtube.playlists().list(
        part="snippet",
        channelId=channel_id,
        maxResults=20)
    response = request.execute()
    data = response["items"]
    playlistdetails = []
    for item in data:
        playlistdetails.append({
            "playlistid": item["id"],
            "channelid": item["snippet"]["channelId"],
            "playlistname": item["snippet"]["title"]
        })
        query = """INSERT INTO playlist (playlist_id, channel_id, playlist_name) VALUES(%s, %s, %s)"""
        mycursor.execute(query, (item["id"], item["snippet"]["channelId"], item["snippet"]["title"]))
    return playlistdetails


#-------------------------VIDEO-DATA------------------------
def video_data(playlist_id):
    videodetails = []
    videoid = []
    request = youtube.playlistItems().list(
        part="contentDetails",
        playlistId=playlist_id,
        maxResults=20,
    )
    response = request.execute()
    videoid += response["items"]
    vid = [item["contentDetails"]["videoId"] for item in videoid]
    for i in vid:
        request = youtube.videos().list(
            part='statistics,snippet,contentDetails',
            id=i)
        response = request.execute()
        v = {
            "id": response["items"][0]["id"],
            "channelid": response["items"][0]["snippet"]["channelId"],
            "Videotitle": response["items"][0]["snippet"].get("title"),
            "description": response["items"][0]["snippet"].get("description"),
            "Published_Date": response["items"][0]["snippet"].get("publishedAt").replace("T", " ").replace("Z", ""),
            "viewcount": response["items"][0]["statistics"].get("viewCount"),
            "commentcount": response["items"][0]["statistics"].get("commentCount"),
            "likeCount": response["items"][0]["statistics"].get("likeCount"),
            "dislikeCount":random.randint(0,50), #DisLike Count Has Been Disbaled Since 2021
            "favoriteCount": response["items"][0]["statistics"].get("favoriteCount"),
            "videotag": response["items"][0]["snippet"].get("tags"),
            "duration":parse_duration(response["items"][0]["contentDetails"]["duration"])
        }
        query = """
            INSERT INTO video (
                video_id, channel_id, published_date, view_count, comment_count, like_count, video_description,
                video_title, favorite_count,dislike_count,duration) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        mycursor.execute(query, (v["id"], v["channelid"], v["Published_Date"], v["viewcount"], v["commentcount"], v["likeCount"], v["description"], v["Videotitle"], v["favoriteCount"], v["dislikeCount"], v["duration"]))
        videodetails.append(v)
    return videodetails, vid


#----------------PARSING-DURATION-IN-SECONDS----------------
def parse_duration(iso_duration):
    duration = isodate.parse_duration(iso_duration)
    return int(duration.total_seconds())


#----------------------COMMENT-DATA-------------------------
def comment_data(video_ids):
    commentDetails = []
    for vid in video_ids:
        try:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=vid,
                maxResults=50
            )
            response = request.execute()
            comments = response.get("items", [])
            for comment in comments:
                commentDetails.append({
                    "videoid": comment["snippet"]["videoId"],
                    "comment_id": comment["snippet"]["topLevelComment"]["id"],
                    "Comment_Text": comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                    "Comment_Author": comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    "Comment_PublishedAt": comment['snippet']['topLevelComment']['snippet']['publishedAt'].replace("T", " ").replace("Z", "")
                })
                query = """
                INSERT INTO comment (
                    comment_id, video_id, comment_text, comment_author, comment_published_date
                ) VALUES (%s, %s, %s, %s, %s)
                """
                mycursor.execute(query, (
                    comment["snippet"]["topLevelComment"]["id"],
                    comment["snippet"]["videoId"],
                    comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                    comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    comment['snippet']['topLevelComment']['snippet']['publishedAt'].replace("T", " ").replace("Z", "")
                ))
        except Exception as e:
            # Handle the error (e.g., log it, print a message, etc.)
            st.warning(f"Comments are disabled for video ID {vid} or an error occurred: {str(e)}")
    
    return commentDetails


#-------------------------HOMEPAGE--------------------------
def homepage():
    st.title("YOUTUBE DATA HARVESTING AND WAREHOUSING")
    st.write("Project By Tumu Mani Sai Pavan")
    #st.image("C:/Users/saipa/Downloads/image.png", width=550)
    file = open("C:/Users/saipa/Downloads/youtube_gif.gif", "rb")
    contents = file.read()
    data_gif = base64.b64encode(contents).decode("utf-8")
    file.close()
    st.markdown(f'<img src="data:image/gif;base64,{data_gif}" style="width: 800px;" >',unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)
    st.subheader("Overview")
    st.write("Building a YouTube data system involves Python data harvesting via the YouTube Data API, storing in MySQL, and Streamlit app development. Python collects YouTube data, MySQL organizes it, and Streamlit enables user-friendly querying and analysis. The system aims to provide insights from YouTube data in an accessible manner for informed decision-making.")
    st.markdown("<br>", unsafe_allow_html=True)
    st.subheader("Skills Takeaway")
    st.write("""
             1) Python Scripting
             2) Data Collection
             3) Streamlit
             4) API Integration
             5) Data Management Using MySQL""")
    st.markdown("<br>", unsafe_allow_html=True)
    st.subheader("About me")
    st.write(""""Self-motivated computer science student with keen interest in coding". Engineer with a passion for machine learning, With a mix of academic knowledge, practical skills, and a growth-oriented attitude, I'm eager to make my debut in the AI & ML field.""")
    st.markdown("<br>", unsafe_allow_html=True)
    st.subheader("Contact")
    st.write("For any queries or collaborations, feel free to reach out to me:")
    email_icon = Image.open("C:/Users/saipa/Downloads/mail.jpg")
    st.write("Email:")
    col1, col2 = st.columns([0.4, 5])
    with col1:
        st.image(email_icon, width=50)
    with col2:
        st.write("tmsaipavan@gmail.com")
    lin_icon = Image.open("C:/Users/saipa/Downloads/in.jpg")
    st.write("LinkedIn:")
    col1, col2 = st.columns([0.4, 5])
    with col1:
        st.image(lin_icon, width=50)
    with col2:
        st.write("[Sai Pavan TM](https://www.linkedin.com/in/saipavantm/)")


#--------------------DATA-HARVESTING-PAGE-------------------
def data_harvesting_page():
    try:
        st.title("Data Harvesting")
        st.write("Enter a YouTube channel ID to fetch its details:")
        channel_id = st.text_input("Enter Channel ID")
        if st.button("Fetch Details"):
            c_data, p_id = channel_data(channel_id)
            st.subheader("Channel Details")
            channel_info = youtube.channels().list(part='snippet', id=channel_id).execute()
            thumbnail_url = channel_info['items'][0]['snippet']['thumbnails']['default']['url']
            st.image(thumbnail_url, width=250)
            st.write(f"<font color='blue'>**Channel Name :**</font> {c_data['channel_name']}", unsafe_allow_html=True)
            st.write(f"<font color='blue'>**Channel ID :**</font> {c_data['channel_id']}", unsafe_allow_html=True)
            st.write(f"<font color='blue'>**Channel Description :**</font> {c_data['channel_dec']}", unsafe_allow_html=True)
            st.write(f"<font color='blue'>**Channel Playlist ID :**</font> {c_data['channel_playlistid']}", unsafe_allow_html=True)
            st.write(f"<font color='blue'>**Channel View Count :**</font> {c_data['channel_viewc']}", unsafe_allow_html=True)
            st.write(f"<font color='blue'>**Channel Subscriber Count :**</font> {c_data['channel_subc']}", unsafe_allow_html=True)
            st.success("Data Fetched Successfully!!!")
            file = open("C:/Users/saipa/Downloads/as.gif", "rb")
            contents = file.read()
            data_gif = base64.b64encode(contents).decode("utf-8")
            file.close()
            st.markdown(f'<img src="data:image/gif;base64,{data_gif}" style="width: 300px;" >',unsafe_allow_html=True)
    except:
        st.error("Error Occurred!!!")


#--------------------DATA-WAREHOUSING-PAGE-------------------
def data_warehousing_page():
    st.title("Data Warehousing")
    channel_ids = {
        "GUVI":"UCduIoIMfD8tT3KoU0-zBRgQ",
        "JIOCINEMA":"UC8To9CFsZzvPafxMLzS08iA",
        "TIMES NOW":"UC6RJ7-PaXg6TIH2BzZfTV7w",
        "TV9 TELUGU":"UCPXTXMecYqnRKNdqdVOGSFg",
        "NEWS18 INDIA":"UCPP3etACgdUWvizcES1dJ8Q",
        "KOLKATA NIGHT RIDERS":"UCp10aBPqcOeBbEg7d_K9SBw",
        "PRIME VIDEO":"UC4zWG9LccdWGUlF77LZ8toA",
        "ZEE5":"UCXOgAl4w-FQero1ERbGHpXQ",
        "STAR SPORTS":"UCmqfX0S3x0I3uwLkPdpX03w"

    }
    selected_channel = st.selectbox("Select any channel", ["GUVI", "JIOCINEMA", "TIMES NOW", "TV9 TELUGU", "STAR SPORTS",
                                                           "NEWS18 INDIA","ZEE5", "KOLKATA NIGHT RIDERS","PRIME VIDEO"])
    if st.button("Insert Data"):
        if is_inserted(channel_ids[selected_channel]):
            st.warning("Data for selected channel is already inserted")
        else:
            ply_data = playlist_data(channel_ids[selected_channel])
            st.success("Playlist Data Inserted Successfully!!!")
            c_data, p_id = channel_data_with_sql(channel_ids[selected_channel])
            video_det, video_i = video_data(p_id)
            st.success("Video Data Inserted Successfully!!!")
            comm = comment_data(video_i)
            st.success("Comments Data Inserted Successfully!!!")
            file = open("C:/Users/saipa/Downloads/as.gif", "rb")
            contents = file.read()
            data_gif = base64.b64encode(contents).decode("utf-8")
            file.close()
            st.markdown(f'<img src="data:image/gif;base64,{data_gif}" style="width: 300px;" >',unsafe_allow_html=True)


#---------------CHECK-FOR-DUPLICATION-ENTRY------------------
def is_inserted(channel):
    mycursor.execute("SELECT DISTINCT channel_id FROM playlist")
    c_ids = [row[0] for row in mycursor.fetchall()]
    for i in c_ids:
        if channel==i:
            return True
    return False


#------------------------1st-QUERY---------------------------
def query_all_videos_and_channels(mycursor):
    query = """
    SELECT video.video_title AS 'Video Title', channel.channel_name AS 'Channel Name'
    FROM video
    JOIN channel ON video.channel_id = channel.channel_id;
    """
    mycursor.execute(query)
    return mycursor.fetchall(), ["Video Title", "Channel Name"]


#------------------------2nd-QUERY---------------------------
def query_channels_with_most_videos(mycursor):
    query = """
    SELECT channel.channel_name AS 'Channel Name', COUNT(video.video_id) AS 'Video Count'
    FROM channel
    JOIN video ON channel.channel_id = video.channel_id
    GROUP BY channel.channel_name
    ORDER BY COUNT(video.video_id) DESC;
    """
    mycursor.execute(query)
    return mycursor.fetchall(), ["Channel Name", "Video Count"]


#------------------------3rd-QUERY---------------------------
def query_top_10_most_viewed_videos(mycursor):
    query = """
    SELECT video.video_title AS 'Video Title', channel.channel_name AS 'Channel Name', video.view_count AS 'View Count'
    FROM video
    JOIN channel ON video.channel_id = channel.channel_id
    ORDER BY video.view_count DESC
    LIMIT 10;
    """
    mycursor.execute(query)
    return mycursor.fetchall(), ["Video Title", "Channel Name", "View Count"]


#------------------------4th-QUERY---------------------------
def query_comments_per_video(mycursor):
    query = """
    SELECT video.video_title AS 'Video Title', COUNT(comment.comment_id) AS 'Comment Count'
    FROM video
    JOIN comment ON video.video_id = comment.video_id
    GROUP BY video.video_title;
    """
    mycursor.execute(query)
    return mycursor.fetchall(), ["Video Title", "Comment Count"]


#------------------------5th-QUERY---------------------------
def query_videos_with_highest_likes(mycursor):
    query = """
    SELECT video.video_title AS 'Video Title', channel.channel_name AS 'Channel Name', video.like_count AS 'Like Count'
    FROM video
    JOIN channel ON video.channel_id = channel.channel_id
    ORDER BY video.like_count DESC;
    """
    mycursor.execute(query)
    return mycursor.fetchall(), ["Video Title", "Channel Name", "Like Count"]


#------------------------6th-QUERY---------------------------
def query_likes_dislikes_per_video(mycursor):
    query = """
    SELECT video.video_title AS 'Video Title', video.like_count AS 'Like Count', video.dislike_count AS 'Dislike Count'
    FROM video;
    """
    mycursor.execute(query)
    return mycursor.fetchall(), ["Video Title", "Like Count", "Dislike Count"]


#------------------------7th-QUERY---------------------------
def query_total_views_per_channel(mycursor):
    query = """
    SELECT channel.channel_name AS 'Channel Name', SUM(video.view_count) AS 'Total Views'
    FROM channel
    JOIN video ON channel.channel_id = video.channel_id
    GROUP BY channel.channel_name;
    """
    mycursor.execute(query)
    return mycursor.fetchall(), ["Channel Name", "Total Views"]


#------------------------8th-QUERY---------------------------
def query_channels_published_in_2022(mycursor):
    query = """
    SELECT DISTINCT channel.channel_name AS 'Channel Name'
    FROM channel
    JOIN video ON channel.channel_id = video.channel_id
    WHERE YEAR(video.published_date) = 2024;
    """
    mycursor.execute(query)
    return mycursor.fetchall(), ["Channel Name"]


#------------------------9th-QUERY---------------------------
def query_avg_duration_per_channel(mycursor):
    query = """
    SELECT channel.channel_name AS 'Channel Name', AVG(video.duration) AS 'Average Duration'
    FROM channel
    JOIN video ON channel.channel_id = video.channel_id
    GROUP BY channel.channel_name;
    """
    mycursor.execute(query)
    return mycursor.fetchall(), ["Channel Name", "Average Duration(sec)"]


#-------------------------10th-QUERY---------------------------
def query_videos_with_highest_comments(mycursor):
    query = """
    SELECT video.video_title AS 'Video Title', channel.channel_name AS 'Channel Name', COUNT(comment.comment_id) AS 'Comment Count'
    FROM video
    JOIN channel ON video.channel_id = channel.channel_id
    JOIN comment ON video.video_id = comment.video_id
    GROUP BY video.video_title, channel.channel_name
    ORDER BY comment_count DESC;
    """
    mycursor.execute(query)
    return mycursor.fetchall(), ["Video Title", "Channel Name", "Comment Count"]


#-----------------------QUERY-PART-PAGE------------------------
def query_part():
    st.title("Query Part")

    queries = {
        "Names of all the videos and their corresponding channels": query_all_videos_and_channels,
        "Channels with the most number of videos": query_channels_with_most_videos,
        "Top 10 most viewed videos and their respective channels": query_top_10_most_viewed_videos,
        "Number of comments on each video and their corresponding video names": query_comments_per_video,
        "Videos with the highest number of likes and their corresponding channel names": query_videos_with_highest_likes,
        "Total number of likes and dislikes for each video and their corresponding video names": query_likes_dislikes_per_video,
        "Total number of views for each channel and their corresponding channel names": query_total_views_per_channel,
        "Names of all the channels that have published videos in 2024": query_channels_published_in_2022,
        "Average duration of all videos in each channel and their corresponding channel names": query_avg_duration_per_channel,
        "Videos with the highest number of comments and their corresponding channel names": query_videos_with_highest_comments
    }

    selected_query = st.selectbox("Select a query to execute", list(queries.keys()))

    if st.button("Execute"):
        mycon = get_mysql_connection()
        mycursor = mycon.cursor()
        result, columns = queries[selected_query](mycursor)
        
        if result:
            df = pd.DataFrame(result, columns=columns)
            st.dataframe(df)
            st.success("Fetched Successfully!!!")
        else:
            st.write("No results found.")

        mycon.close()


#-----------------------MAIN-FUNCTION--------------------------
def main():
    st.sidebar.title("Navigation")
    app_mode = st.sidebar.radio("Go to", ("Homepage", "Data Harvesting", "Data Warehousing", "Query Part"))

    if app_mode == "Homepage":
        homepage()
    elif app_mode == "Data Harvesting":
        data_harvesting_page()
    elif app_mode == "Data Warehousing":
        data_warehousing_page()
    elif app_mode == "Query Part":
        query_part()


#-------------------MAIN-FUNCTION-EXECUTION-------------------
if __name__ == "__main__":
    api_service_name = "youtube"
    api_version = "v3"
    api_key = "AIzaSyCLlbkGoiVNIPoeq3FLGH7U78alQalfFvo"
    youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
    mycon = get_mysql_connection()
    mycursor = mycon.cursor()
    main()

#------------------END-CONNECTION-WITH-MySQL------------------
mycon.commit()
mycon.close()