Introduction:

This project involves creating a user-friendly Streamlit application that utilizes the Google API to collect insightful data from YouTube channels. The acquired data is stored in MongoDB, then transitioned to a SQL data warehouse for analysis and exploration, all accessible through the Streamlit app.

Project Overview:

The YouTube Data Harvesting and Warehousing project consists of the following components:

Streamlit Application: A user-friendly UI built using Streamlit library, allowing users to interact with the application and perform data retrieval and analysis tasks.
YouTube API Integration: Integration with the YouTube API to fetch channel and video data based on the provided channel ID.
MongoDB : Storage of the retrieved data in a MongoDB database, providing a flexible and scalable solution for storing unstructured and semi-structured data.
SQL Data Warehouse: Migration of data from the MongoDB to a SQL database, allowing for efficient querying and analysis using SQL queries.

Technologies Used:

•	Python scripting
•	Data Collection
•	API integration
•	Data Management using MongoDB and SQL
•	Streamlit

Installation:

To run this project, you need to install the following packages:

pip install google-api-python-client
pip install pymongo
pip install pandas
pip install psycopg2
pip install streamlit

Imports:

•	from googleapiclient.discovery import build
•	import googleapiclient.discovery
•	import pymongo
•	import psycopg2
•	import pandas as pd
•	import streamlit as st

Features:

Retrieve data from the YouTube API, including channel information, videos, and comments.
Store the retrieved data in a MongoDB database. Option to check wheather the respective channel Data is exist or Not in MongoDb.
Migrate the data to a MySQL data warehouse.
Analyze data using Streamlit.
Perform queries on the MySQL data warehouse.
Display the list of channel name's along with channel id, views, subscription, likes,comments.

Retrieving data from the YouTube API

The project utilizes the Google API to retrieve comprehensive data from YouTube channels. The data includes information on channels, playlists, videos, and comments.

Storing data in MongoDB

The retrieved data is stored in a MongoDB database based on channel Id. Before storing the data in mongodb we used the one of the Option to check wheather the respective channel Data is exist or Not in MongoDb.

Migrating data to a SQL data warehouse

The application allows users to migrate data from MongoDB to a SQL data warehouse. Users can given the input as channel id to migrate the data to sql database. To ensure compatibility with a structured format, the data is cleansed using the powerful pandas library. Following data cleaning, the information is segregated into separate tables, including channels, videos, and comments, utilizing SQL queries.

Analysis

The project provides comprehensive data analysis capabilities using Streamlit. The Streamlit app's user-friendly interface allows for interactive exploration and customization of these  features provided to Perform data analysis and Support for handling multiple YouTube channels and managing their data.

Conclusion:

In summary, this project leverages the Google API to collect, store, and analyze data from YouTube channels, making it accessible through a user-friendly Streamlit application. With MongoDB for initial storage and SQL for structured data warehousing, users can seamlessly transition and explore data. This comprehensive approach streamlines the entire process, making it a valuable tool for data enthusiasts and analysts. The project offers flexibility, scalability, and data analyses users to gain insights from the vast amount of YouTube data available.

User streamlit

 Local URL: http://localhost:8501
 Network URL: http://192.168.100.4:8501
