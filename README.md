**_Flight Delay & Cancellation_**

# **The Problem Statement**

The U.S. National airspace system works in high-frequency, so when one plane is late for its first flight of the morning. That same plane has to fly 5 more times that day because it started late, every single flight after it is now late too. This is called a Propagated Delay. Now the question is, how much extra buffer time do need to stop one late flight form ruining the whole day for that plane?

Now we need distributed processing. We can't do this on our laptops with normal tools like standard python and the reasons are:

## 1-**_High Volume_**

Airports contain 10 years of flights data. That can go over 60 million rows so if we try to open that in excel our computer will stop working. That's where we need Spark cause it breaks these massive amounts of data into smaller chunks to process all at once.

## 2-**_Different Varieties_**

We have a mix of different types of data for example: flight logs which are a list of who was late and when are in CSV format which we will use Apache Spark for it since it is heavy duty processing of the 60-million-row flight logs, Weather Reports that show if it was snowing or raining during these flights are in JSON format which we will also use Apache Spark for, plane info that contain the tech specs of the specific planes are in Parquet format where we will use pandas for since it is smaller semi-structured metadata.

## 3-**_Velocity_**

If the airline wants to fix a delay which is happening now they can't wait for 5 hours until the script runs so our pipeline will show how we are gonna process millions of data in just minutes.

**Pipeline**

Our pipeline will be fully automated and created using Apache Airflow. It will manage the workflow, them running Spark jobs link weather data to specific flights. Once it gets transformed the data is stored in parquet format for better storage. The pipeline will include automated quality checks:

**_Schema Validation:_** The system makes sure the incoming CSV and JSON files match the expected format before it processes it.

**_Null handling:_** we are going to implement error handling to catch missing weather data or canceled flight status to prevent any errors.

**_Alerting:_** Airflow is made to give alerts if a task fails ensuring we can fix the data issues immediately.

**Motivation**

Delay minutes cause a huge financial impact by costing the U.S. airlines around 100\$ per minute in fuel, overtime for the crew, and also passengers rebooking. Airlines need Operational Intelligence since they currently have plenty of data but they lack getting insights on time for it to fix its schedules. And by identifying the point where a small delay becomes a major disaster these airlines can work their schedules to be better during storm seasons without wasting money on buffer time during the clear weather.

**Expected Analysis Delivery**

The final output will be an automated Dashboard this dashboard will visualize the results of our analysis. It will show which combination of Airport + Weather Condition + Plane Tech creates highest risk of a chain reaction of delays which allows airline managers to make better scheduling decisions. Business insights will be as such:

**_Delay Correlation:_** it will show which specific weather conditions have the highest correlation with a 15 minute delay turning into a 2 hour delay.

**_Efficiency:_** this will show the top 5 airports that currently have the least amount of buffer time built into their schedules.

**_Impact:_** which will talk about do older aircraft models have higher rates of mechanical related delays?