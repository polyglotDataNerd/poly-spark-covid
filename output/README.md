**Tracks COVID19 Cases**

Output sample

Output Fields
-
    name:string
    level:string
    city:string
    county:string
    state:string
    country:string
    population:string
    latitude:double
    longitude:double
    url:string
    aggregate:string
    timezone:string
    cases:string
    us_confirmed_county:string
    deaths:string
    us_deaths_county:string
    recovered:string
    us_recovered_county:string
    active:string
    us_active_county:string
    tested:string
    hospitalized:string
    discharged:string
    growth_factor:string
    last_updated:date
    
Output Sample Query Result Sets
-
* Refer to compress gzip in the directory

        +------------+-----------+
        |last_updated|cds_records|
        +------------+-----------+
        |2020-04-12  |2868       |
        |2020-04-11  |2850       |
        |2020-04-10  |2793       |
        |2020-04-09  |2769       |
        |2020-04-08  |2747       |
        +------------+-----------+
        only showing top 5 rows
        
        +------------+-----------+
        |last_updated|jhu_records|
        +------------+-----------+
        |2020-04-12  |2719       |
        |2020-04-11  |2696       |
        |2020-04-10  |2672       |
        |2020-04-09  |2642       |
        |2020-04-08  |2614       |
        +------------+-----------+
        only showing top 5 rows
        
        +-----------+---------+-----------+
        |Last_Update|us_deaths|us_affected|
        +-----------+---------+-----------+
        |2020-04-12 |22,018   |555,249    |
        |2020-04-11 |20,463   |526,396    |
        |2020-04-10 |18,586   |496,535    |
        |2020-04-09 |16,478   |461,437    |
        |2020-04-08 |14,695   |429,052    |
        +-----------+---------+-----------+
        only showing top 5 rows
        
        +------------+----------------+
        |last_updated|combined_records|
        +------------+----------------+
        |2020-04-12  |2868            |
        |2020-04-11  |2850            |
        |2020-04-10  |2793            |
        |2020-04-09  |2769            |
        |2020-04-08  |2747            |
        +------------+----------------+
        only showing top 5 rows
        
        +------------+--------------+--------+------+---------+------------+----------+
        |last_updated|state         |infected|deaths|recovered|hospitalized|discharged|
        +------------+--------------+--------+------+---------+------------+----------+
        |2020-04-12  |New York      |188,694 |2,447 |null     |null        |null      |
        |2020-04-12  |New Jersey    |60,576  |2,339 |null     |null        |null      |
        |2020-04-12  |Massachusetts |25,475  |743   |null     |null        |null      |
        |2020-04-12  |Michigan      |24,473  |1,456 |null     |null        |null      |
        |2020-04-12  |California    |23,292  |184   |900      |null        |null      |
        |2020-04-12  |Pennsylvania  |22,833  |557   |null     |null        |null      |
        |2020-04-12  |Illinois      |20,852  |708   |null     |null        |null      |
        |2020-04-12  |Louisiana     |20,595  |791   |null     |null        |null      |
        |2020-04-12  |Florida       |19,895  |367   |null     |null        |null      |
        |2020-04-12  |Texas         |13,484  |264   |null     |null        |null      |
        |2020-04-12  |Georgia       |12,550  |429   |null     |null        |null      |
        |2020-04-12  |Connecticut   |11,534  |424   |null     |null        |null      |
        |2020-04-12  |Washington    |9,792   |506   |null     |null        |null      |
        |2020-04-12  |Maryland      |8,225   |160   |null     |null        |null      |
        |2020-04-12  |Indiana       |7,928   |340   |null     |null        |null      |
        |2020-04-12  |Colorado      |7,147   |252   |null     |null        |null      |
        |2020-04-12  |Ohio          |6,604   |253   |null     |null        |null      |
        |2020-04-12  |Virginia      |5,274   |94    |null     |null        |null      |
        |2020-04-12  |Tennessee     |5,057   |105   |1,497    |null        |null      |
        |2020-04-12  |North Carolina|4,520   |89    |null     |null        |null      |
        |2020-04-12  |Missouri      |4,145   |36    |null     |null        |null      |
        |2020-04-12  |Alabama       |3,583   |93    |null     |null        |null      |
        |2020-04-12  |Arizona       |3,539   |93    |null     |null        |null      |
        |2020-04-12  |Wisconsin     |3,341   |142   |null     |null        |null      |
        |2020-04-12  |South Carolina|3,319   |82    |null     |null        |null      |
        |2020-04-12  |Nevada        |2,860   |112   |66       |null        |null      |
        |2020-04-12  |Mississippi   |2,781   |89    |null     |null        |null      |
        |2020-04-12  |Utah          |2,701   |7     |null     |null        |null      |
        |2020-04-12  |Kentucky      |2,001   |69    |271      |null        |null      |
        |2020-04-12  |Oklahoma      |1,970   |96    |null     |null        |null      |
        |2020-04-12  |Rhode Island  |1,686   |3     |null     |null        |null      |
        |2020-04-12  |Delaware      |1,625   |14    |null     |null        |null      |
        |2020-04-12  |Minnesota     |1,616   |65    |null     |null        |null      |
        |2020-04-12  |Iowa          |1,587   |35    |null     |null        |null      |
        |2020-04-12  |Oregon        |1,527   |52    |null     |null        |null      |
        |2020-04-12  |Idaho         |1,426   |20    |null     |null        |null      |
        |2020-04-12  |Kansas        |1,337   |52    |null     |null        |null      |
        |2020-04-12  |Arkansas      |1,280   |25    |367      |null        |null      |
        |2020-04-12  |New Mexico    |1,245   |23    |null     |null        |null      |
        |2020-04-12  |New Hampshire |985     |3     |null     |null        |null      |
        |2020-04-12  |Vermont       |727     |27    |null     |null        |null      |
        |2020-04-12  |Nebraska      |725     |15    |null     |null        |null      |
        |2020-04-12  |Maine         |633     |19    |266      |null        |null      |
        |2020-04-12  |West Virginia |611     |4     |null     |null        |null      |
        |2020-04-12  |Hawaii        |494     |9     |null     |null        |null      |
        |2020-04-12  |Wyoming       |364     |0     |null     |null        |null      |
        |2020-04-12  |Montana       |332     |6     |null     |null        |null      |
        |2020-04-12  |North Dakota  |308     |7     |null     |null        |null      |
        |2020-04-12  |Alaska        |255     |4     |null     |null        |null      |
        |2020-04-12  |South Dakota  |197     |6     |null     |null        |null      |
        +------------+--------------+--------+------+---------+------------+----------+