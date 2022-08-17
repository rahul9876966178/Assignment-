# Databricks notebook source
# importing  PySpark and loading the  ipl datasets

#pip intall spark
import spark
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("") \
    .getOrCreate()

# loading the dataset having three files
#spark.read.csv methods take a file path to read from as an argument
#If you have a header with column names on file, you need to explicitly specify true for header option using option("header",true) else it will be false
#It also reads all columns as a string (StringType) by default 

df = spark.read.csv('/FileStore/tables/WC_ball_by_ball.csv',header=True)
df1 = spark.read.csv('/FileStore/tables/WC_matches.csv',header=True)
df2 = spark.read.csv('/FileStore/tables/WC_venue.csv',header=True)

# USING SQL COMMANDS

df.createOrReplaceTempView("balls")
spark.sql(
"""
select * from balls
"""
)

df1.createOrReplaceTempView("matches")
spark.sql(
"""
select * from matches
"""
)

df2.createOrReplaceTempView("venue")
spark.sql(
"""
select * from venue
"""
)
df.show(vertical=True)
df1.show(vertical=True)
df2.show()

# Data preparation and cleaning

#Lets convert some columns to integer type dataypes
from pyspark.sql.types import IntegerType
df = df.withColumn("is_wicket", df["is_wicket"].cast(IntegerType()))
df = df.withColumn("batsman_runs", df["batsman_runs"].cast(IntegerType()))
df = df.withColumn("extra_runs", df["extra_runs"].cast(IntegerType()))
df = df.withColumn("total_runs", df["total_runs"].cast(IntegerType()))

# lets have a look at the summary of the data
df.summary().show(vertical=True)

# checking for missing or nan values in the dataset

from pyspark.sql.functions import isnan, when, count, col
df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]).show(vertical=True) # for df dataframe

from pyspark.sql.functions import isnan, when, count, col
df1.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df1.columns]).show(vertical=True) # for df1
# no missing values in dataset hence, moving ahead

#Doing  the Analysis on the WC dataset

#Find venue details of the match where V Kohli scored his highest individual runs in IPL

spark.sql(
"""
select m.match_id,first(v.venue),first(v.city),sum(b.batsman_runs) from balls as b
inner join matches as m on b.match_id=m.match_id
inner join venue as v on m.venue_id =v.venue_id
where b.batsman= 'V Kohli'
group by m.match_id
order by sum(b.batsman_runs) desc limit 1; 
""").show()
#First filtering in matches in which batsman is kohli and then
#grouping by each match and finding total runs by each match 
#returning them in descending order and limit by 1 because 0 means no run at all


# Write a query to find out who has officiated (as an umpire) the most number of matches in WC
spark.sql(
"""
select umpire, count(*)
from ((select umpire1 as umpire from matches) union all
      (select umpire2 from matches)
     ) matches
group by umpire
order by count(*) desc ;
""").show()
#here I am just Combing two similar columns by union  and counting numbers of times they have officiated as an umpire

#top 3 venues which hosted the most number of eliminator matches
#spark.sql(
"""
select b.venue,count(b.venue_id) from matches a
inner join venue b on a.venue_id=b.venue_id
group by b.venue
order by 2 desc;
""").show(20)


#return most number of catches taken by a player in IPL history
spark.sql(
"""
select fielder,count(*) from balls
where dismissal_kind == 'caught'
group by fielder
order by count(1) desc
"""
).show()
#only showing top 20 rows
#KD kartik has taken 118 catches in the ipl matches

#write a query to return a  report for highest wicket taker in matches
spark.sql(
"""
select batsman,count(*) from balls
where dismissal_kind == 'hit wicket'
group by batsman
order by count(1) desc
"""
).show()

#Write a query to return a report for highest wicket taker in matches which were affected by Duckworth-Lewis’s method (D/L method).

merged=df.join(df1,on='match_id')#merging df and df1 tables 
DL=merged[merged['method']=='D/L']#finding all matches which were effected by D/L method
sum1=DL.groupBy(['match_id','batsman']).sum('hit_wicket')#finding out total wicket taken by every batsman in each match effected by D/L 


from pyspark.sql.window import Window
from pyspark.sql.functions import rank
from pyspark.sql.functions import col


window = Window.partitionBy('match_id').orderBy(col('sum(hit_wicket)').desc())
res_4=sum1.withColumn('rank',rank().over(window))
res_4.filter(res_4.rank==1).show()

#Write a query to return a report for highest strike rate by a batsman in non powerplay (7-20 overs)

legal=df[(~df['extras_type'].isin(['wides', 'noballs'])) & (df['overs'].between(6,19)) ]#filtering out wides and no balls, and taking  only non powerplay overs(7-20) 
runs=legal.groupBy('batsman').sum('batsman_runs')#finding out total runs by each batsman
ball=legal.groupby('batsman').count()#finding out total balls faced by each batsman

joined=balls.join(runs, balls.batsman_1 == runs.batsman, 'inner')
#In pandas we could  just do strike_rate=runs*100/balls and we could get strike rate. but I can't do same in pyspark , so first I will join the two result set and then find the strike rate
result_4=joined.withColumn("Strike rate", col("sum(batsman_runs)")*100/col("count")).sort('Strike rate',ascending=False)#Dividing the two columns and getting the strike rate the hard way.
result_4.select('batsman','Strike rate').show(10)#Top 10 batsman with highest strike in non powerplay

#write a query to return a report of cricketers with the most number of players of match awards in netural venue?
spark.sql(
"""
select player_of_match,count(player_of_match) from matches
where neutral_venue = 1
group by player_of_match
order by count(player_of_match) desc 
""").show()

#Write a query to get a list of top 10 players with the highest batting average

player_dismissed=df.groupby('player_dismissed').count()#Counting number of times a player is dismissed in a match
total_runs=df.groupBy('batsman').sum('batsman_runs') # grouping
joined_data=player_dismissed.join(total_runs, player_dismissed.player_dismissed == total_runs.batsman, 'inner') # using inner join

result_7=joined_data.withColumn("Batsmen Avg", col("sum(batsman_runs)")/col("count")).sort('Batsmen Avg',ascending=False)#Dividing runs by number of times they have been dismissed
result_7.select('batsman','Batsmen Avg').show(10) # only showing top 10

# creative case study


spark.sql(
"""
select toss_winner,count(toss_winner) from matches
group by toss_winner
order by count(toss_winner) desc 
""").show()

#Which team has won most number of matches in IPL
spark.sql(
"""
select winner,count(winner) from matches
group by winner
order by count(winner) desc 
""").show()

#So, On the basis if the results I can conclude that yes certainly there is some impact on winning and lossing of tosses overs matches but not that much.

# SECTION:3 USING SQLITE

class Database:

    def __init__(self, db_name='Entire_IPL_Data',):
        self.name = db_name
        self.conn = sqlt.connect('Entire_dataset_table.db')
        
        try:
          self.entire = Entire_IPL_Data.to_sql('Entire_dataset_table', conn)
        except:
          pass  
        
    def get_status(self): # To get the status of Sqlite database connection
      return self.conn   
      
    def Query_A(self):
      x = sqlContext.sql('select m.match_id,first(v.venue),first(v.city),sum(b.batsman_runs) from balls as b\
inner join matches as m on b.match_id=m.match_id\
inner join venue as v on m.venue_id =v.venue_id\
where b.batsman= 'V Kohli'\
group by m.match_id\
order by sum(b.batsman_runs) desc limit 1')
      
      dict1 = {} # Creating a Dictionary python object
      
      x = x.toPandas()
  
      for column in x.columns:
         dict1[column] = x[column].values.tolist() # Traverse through each column
      return dict1
    def Query_B(self):
      x = sqlContext.sql('select umpire, count(*)\
from ((select umpire1 as umpire from matches) union all\
(select umpire2 from matches)\
     ) matches\
group by umpire\
order by count(*) desc')
      
      dict1 = {} # Creating a Dictionary python object
      
      x = x.toPandas()
  
      for column in x.columns:
         dict1[column] = x[column].values.tolist()
        
      return dict1
       
    def Query_C(self):
      x = sqlContext.sql('select b.venue,count(b.venue_id) from matches a\
 inner join venue b on a.venue_id=b.venue_id\
group by b.venue\
order by 2 desc')
      
      dict1 = {}
      x = x.toPandas()
      for column in x.columns:
         dict1[column] = x[column].values.tolist() # Traverse through each column
          
      return  dict1
    
    
    def __del__(self):
        self.conn.close()
        
Db = Database() # Object of class Database to implement SQL queires using class methods





