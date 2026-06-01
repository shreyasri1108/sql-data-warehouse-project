# sql-data-warehouse-project
Building Datawarehouse project using SQL server , including ETL process , data modeling and analytics , using application of ( Bronze , Silver and Gold Layers)

Now here the projects are divided into 3 segments
1 - Bronze Layer - At this step the RAW data has been loaded into Database
2 - Silver Layer - One of the most important layer , where we have transformed the whole data , removed unneccesary and duplicate values , and provided value to it 
3 - Gold Layer - Is cleaned data , Here the bussiness ready data has been created to be used by analystics team for bussiness analysis .

<h2> Star Schema <p> -> is used for Data modeling , the create relationships between FACT table and DIMENSION table </p> </h2>

FACT table - has all the foreign keys related to dimensions
DIMENSION table - has the primary keys along with the values 

Using above , establishing relationship between FACT and DIMENSION table , eases DATA MINING
