**Project5:**

**Big data Analytics in retail**

**Customer is the king**

The experience of shopping has changed dramatically in recent years

The power has shifted to consumers.

Buyers can easily research and compare products from any device, from
anywhere even within the shop using his mobile.

Shoppers can share their reviews about retailers and products through
social media and influence other prospective buyers.

**What can the retailers do?**

What can retailers do in the new multi-channel environment where all
data is at the fingertips for the buyers?

Retailers have to employ new strategies to attract and retain
customers.

Big data and Hadoop enable retailers to connect with customers through
multiple channels at an entirely new level by harnessing the massive
volumes of new data available today.

They can use Big data analytics to get a whole lot of information that
can steer their business.

**Analytics to help retailers**

Below are a few analytics and use cases in which Hadoop can be used to
develop closer relationships with customers, be more competitive, and
create entirely new kinds of shopping experience for the customers.

**Up-Sell/Cross-Sell Recommendations**

Providing up-sell and cross-sell recommendations to customers is the
mostly widely adopted big data use case in the retail sector. This
enables retailers to increase online purchases by recommending relevant
products and promotions in real time.

**Social Media Analysis**

Consumers can use social media to exert tremendous influence over a
retailers brand or a product success. Retailers need to monitor online
sentiment and respond in real time with relevant messages or offers and
need to use social media to effectively attract customers.

**Dynamic Pricing Across Multiple Channels**

When consumers are able to shop across multiple channels in real time,
slight differences in pricing can make a difference in their purchase
decisions. Big data allows for a more refined set of indicators for
price elasticity in comparison with traditional influencers such as time
and availability. Using effective pricing can win and retain customers.

**Other major analytics:**

**Fraud detection**

**Click stream analysis**

**Loyalty programs**

**Know your customer programs**

**Steps involved:**

Here are the high level steps involved in recommending up-sale or
cross-sale for customers:

Collect customer data

Collect past history and customer profile data

Analyze spending capacity and pattern of customer

Choose potential customers that will spend

Identify product recommendations for customers

**Input Data:**

Customer\_Cross\_Sell.csv:

  ---------------- ----------- ------- ----------- ------- ------- ------ --------- -------- --------------- ---------------------
  **First name**   Last name   Email   Photo URL   State   Phone   Date   Product   Amount   Product\_code   Quantity\_purchased
  ---------------- ----------- ------- ----------- ------- ------- ------ --------- -------- --------------- ---------------------

***Solution:***

Create a table in Hive and upload the customer data

-- create hive table for the downloaded data

CREATE TABLE IF NOT EXISTS customer\_data (

fname string,

lname string,

email string,

photo string,

state string,

phone string,

date string,

product string,

amount double,

prod\_code bigint,

quantity int

)

ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (

"separatorChar" = ",",

"quoteChar" = "\\""

)

STORED AS TEXTFILE

TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/cloudera/input/Customer\_Cross\_sell.csv'
OVERWRITE INTO TABLE customer\_data;

-- clean up the data, remove null values

CREATE TABLE IF NOT EXISTS cust\_data AS

SELECT \* FROM customer\_data

WHERE product != '';

-- create view to categorize the products

CREATE VIEW IF NOT EXISTS prod\_category AS

SELECT DISTINCT(product) AS products, prod\_code,

CASE WHEN product IN ('Almond','Chicken whole','Chocolates
HAK','Poptarts','Spice and
Herbs','Oranges','Sugar','Chicken','Carrots','Potato
chips','Cauliflower','Wheat Bread','Bread','Green
grapes','Papaya','Beets','Black grapes','Cookies') THEN **'Groceries'**

WHEN product IN ('Yogurt','Butter','Milk') THEN **'Dairy'**

WHEN product IN ('Mountain Dew','Coffee ground','Coke') THEN
**'Beverages'**

WHEN product IN ('T shirt','Leather shoes','Hair net','Pants') THEN
**'Clothing'**

WHEN product IN ('Playing cards','Ankle bands','Tylenol','Running
Shoes','Multivitamins','Fishing net') THEN **'Sports'**

WHEN product IN ('CD ROMS','Electric guitar','Drumsticks','Flute') THEN
**'Music'**

WHEN product IN ('Shaving Gel') THEN **'Grooming'**

WHEN product IN ('Stapler pins','Pencils','Pens','Stapler') THEN
**'Stationery'**

WHEN product IN ('Silver knives','Spoons','Forks') THEN 'Kitchen'

ELSE **'Others'**

END AS category

FROM cust\_data;

Sample output:

![](./screenshots/media/image1.tiff)

Let’s now analyse the spending capacity and purchase pattern of the
customers. We will assign each customer a category and find the total
spend per category.

-- create view to prepare for analysis

CREATE VIEW IF NOT EXISTS customer\_shopping\_pattern AS

SELECT CONCAT(cd.fname,' ',cd.lname) AS name,

pc.category category,

SUM(cd.amount\*cd.quantity) AS total\_spend

FROM cust\_data cd

INNER JOIN prod\_category pc

ON cd.prod\_code = pc.prod\_code

GROUP BY CONCAT(cd.fname,' ',cd.lname),pc.category

DISTRIBUTE BY name

SORT BY total\_spend DESC;

Sample Output:

![](./screenshots/media/image2.tiff)

Now, let’s collate all the categories that the customer is buying along
with the spends in each category and assign it to the respective
customers. We will get a glimpse into every customer’s buying pattern
and spend capacity. Basis this data we can recommend a category to every
customer who can potentially become our up-sell, cross-sell customers.

-- create final table for analysis

CREATE TABLE IF NOT EXISTS customer\_spend\_analysis AS

SELECT name,

COLLECT\_LIST(category) AS categories,

COLLECT\_LIST(total\_spend) AS category\_total\_spend,

SUM(total\_spend) AS total\_spent,

COLLECT\_LIST(category)\[0\] AS recommended\_category

FROM customer\_shopping\_pattern

GROUP BY name;

Sample output:

![](./screenshots/media/image3.tiff)

![](./screenshots/media/image4.tiff)

Now, we can also choose the potential customers who are likely to spend
more.

-- identify potential customers for up-sell

CREATE TABLE IF NOT EXISTS upsell\_potential AS

SELECT name,recommended\_category

FROM customer\_spend\_analysis

WHERE total\_spent &gt; 100;

Sample output:

![](./screenshots/media/image5.tiff)
