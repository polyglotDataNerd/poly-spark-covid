**Tracks COVID19 Cases**

[Download Consolidated Dataset](https://poly-testing.s3-us-west-2.amazonaws.com/covid/combined/covid19_combined.gz)

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
    icu:string
    hospitalized_current:string
    icu_current:string


Helpful Queries
-

Some important queries to help initial discovery, located in the scala class [Insights](https://github.com/polyglotDataNerd/poly-spark-covid/blob/master/src/main/scala/com/poly/covid/sql/Insights.scala). The platform is Apache Spark and the language is Scala but if you are more familiar with PySpark the queries could be used in any Spark interpreter.  

Some useful queries in the Insights class for the USA

 1. [Deaths and affected by COVID by day US totals](https://github.com/polyglotDataNerd/poly-spark-covid/blob/master/src/main/scala/com/poly/covid/sql/Insights.scala#L45): this is an exact match John Hopkins Outputs in their daily graph. 
    
        +------------+---------+-----------+
        |last_updated|us_deaths|us_affected|
        +------------+---------+-----------+
        |2020-06-20  |119,118  |2,220,962  |
        |2020-06-19  |118,440  |2,191,053  |
        |2020-06-18  |117,717  |2,163,290  |
        |2020-06-17  |116,963  |2,137,731  |
        |2020-06-16  |116,127  |2,114,026  |
        +------------+---------+-----------+
        only showing top 5 rows

 2. [Daily US State Numbers](https://github.com/polyglotDataNerd/poly-spark-covid/blob/master/src/main/scala/com/poly/covid/sql/Insights.scala#L57)
 
        +------------+--------------+--------+------+---------+------------+----------+
        |last_updated|state         |infected|deaths|recovered|hospitalized|discharged|
        +------------+--------------+--------+------+---------+------------+----------+
        |2020-06-19  |New York      |386556  |24686 |null     |null        |null      |
        |2020-06-19  |California    |170727  |5422  |31042    |2540        |null      |
        |2020-06-19  |New Jersey    |167803  |12835 |null     |null        |null      |
        |2020-06-19  |Illinois      |135417  |6580  |null     |null        |null      |
        |2020-06-19  |Massachusetts |106354  |7795  |null     |null        |null      |
        |2020-06-19  |Texas         |102680  |2138  |null     |null        |null      |
        |2020-06-19  |Florida       |89635   |3104  |null     |null        |null      |
        |2020-06-19  |Pennsylvania  |84780   |6408  |null     |null        |null      |
        |2020-06-19  |Michigan      |66825   |6064  |null     |null        |null      |
        |2020-06-19  |Maryland      |63839   |3010  |null     |null        |null      |
        |2020-06-19  |Georgia       |58231   |2615  |null     |9580        |null      |
        |2020-06-19  |Virginia      |56240   |1594  |null     |null        |null      |
        |2020-06-19  |North Carolina|49686   |1197  |null     |null        |null      |
        |2020-06-19  |Louisiana     |48176   |3017  |null     |null        |null      |
        |2020-06-19  |Arizona       |46689   |656   |null     |null        |null      |
        |2020-06-19  |Ohio          |43731   |2667  |null     |null        |null      |
        |2020-06-19  |Connecticut   |43346   |4238  |null     |null        |null      |
        |2020-06-19  |Tennessee     |32604   |509   |null     |null        |null      |
        |2020-06-19  |Minnesota     |31617   |1368  |null     |null        |null      |
        |2020-06-19  |Colorado      |30172   |1643  |null     |null        |null      |
        |2020-06-19  |Alabama       |28583   |817   |null     |null        |null      |
        |2020-06-19  |Washington    |27576   |1250  |null     |null        |null      |
        |2020-06-19  |Iowa          |25156   |680   |15683    |null        |null      |
        |2020-06-19  |Wisconsin     |24154   |730   |null     |null        |null      |
        |2020-06-19  |South Carolina|21548   |630   |null     |null        |null      |
        |2020-06-19  |Mississippi   |20641   |938   |null     |null        |null      |
        |2020-06-19  |Utah          |20466   |83    |null     |null        |null      |
        |2020-06-19  |Nebraska      |17411   |243   |null     |1180        |null      |
        |2020-06-19  |Rhode Island  |14524   |845   |null     |null        |null      |
        |2020-06-19  |Arkansas      |14098   |214   |9506     |null        |null      |
        |2020-06-19  |Kentucky      |13352   |546   |9445     |null        |null      |
        |2020-06-19  |Indiana       |12438   |661   |null     |null        |null      |
        |2020-06-19  |Nevada        |12196   |477   |1422     |null        |174       |
        |2020-06-19  |Kansas        |12059   |127   |null     |null        |null      |
        |2020-06-19  |Missouri      |11680   |676   |2817     |null        |null      |
        |2020-06-19  |New Mexico    |10153   |456   |null     |null        |null      |
        |2020-06-19  |Oklahoma      |9358    |363   |6985     |null        |null      |
        |2020-06-19  |Oregon        |6572    |188   |null     |null        |null      |
        |2020-06-19  |South Dakota  |6109    |79    |null     |null        |null      |
        |2020-06-19  |Delaware      |5909    |317   |null     |null        |null      |
        |2020-06-19  |New Hampshire |5449    |331   |null     |null        |null      |
        |2020-06-19  |Idaho         |3743    |89    |null     |null        |null      |
        |2020-06-19  |North Dakota  |3193    |78    |null     |null        |null      |
        |2020-06-19  |Maine         |2876    |102   |null     |null        |null      |
        |2020-06-19  |West Virginia |2418    |88    |null     |null        |null      |
        |2020-06-19  |Wyoming       |1173    |10    |null     |null        |null      |
        |2020-06-19  |Vermont       |1139    |56    |null     |null        |null      |
        |2020-06-19  |Alaska        |793     |11    |null     |null        |null      |
        |2020-06-19  |Hawaii        |777     |null  |null     |null        |null      |
        |2020-06-19  |Montana       |332     |10    |null     |null        |null      |
        +------------+--------------+--------+------+---------+------------+----------+

  3. [State Day over Day delta's Deaths and Affected](https://github.com/polyglotDataNerd/poly-spark-covid/blob/master/src/main/scala/com/poly/covid/sql/Insights.scala#L92)

            +------------+-------+--------+------------+------+----------+
            |last_updated|state  |infected|dod_infected|deaths|dod_deaths|
            +------------+-------+--------+------------+------+----------+
            |2020-06-19  |Florida|89635   |3819        |3104  |43        |
            |2020-06-18  |Florida|85816   |5804        |3061  |68        |
            |2020-06-17  |Florida|80012   |8           |2993  |1         |
            |2020-06-16  |Florida|80004   |2775        |2992  |54        |
            |2020-06-15  |Florida|77229   |1763        |2938  |7         |
            |2020-06-14  |Florida|75466   |2016        |2931  |6         |
            |2020-06-13  |Florida|73450   |4477        |2925  |77        |
            |2020-06-12  |Florida|68973   |0           |2848  |0         |
            |2020-06-11  |Florida|68973   |1698        |2848  |47        |
            |2020-06-10  |Florida|67275   |1368        |2801  |36        |
            |2020-06-09  |Florida|65907   |1104        |2765  |54        |
            |2020-06-08  |Florida|64803   |4710        |2711  |104       |
            |2020-06-07  |Florida|60093   |0           |2607  |0         |
            |2020-06-06  |Florida|60093   |0           |2607  |0         |
            |2020-06-05  |Florida|60093   |0           |2607  |0         |
            |2020-06-04  |Florida|60093   |1418        |2607  |41        |
            |2020-06-03  |Florida|58675   |1315        |2566  |36        |
            |2020-06-02  |Florida|57360   |614         |2530  |70        |
            |2020-06-01  |Florida|56746   |675         |2460  |10        |
            |2020-05-31  |Florida|56071   |731         |2450  |3         |
            |2020-05-30  |Florida|55340   |934         |2447  |35        |
            |2020-05-29  |Florida|54406   |1207        |2412  |48        |
            |2020-05-28  |Florida|53199   |650         |2364  |45        |
            |2020-05-27  |Florida|52549   |387         |2319  |61        |
            |2020-05-26  |Florida|52162   |501         |2258  |6         |
            +------------+-------+--------+------------+------+----------+
            only showing top 25 rows
            
                
            +------------+-----+--------+------------+------+----------+
            |last_updated|state|infected|dod_infected|deaths|dod_deaths|
            +------------+-----+--------+------------+------+----------+
            |2020-06-19  |Texas|102680  |1           |2138  |17        |
            |2020-06-18  |Texas|102679  |3211        |2121  |41        |
            |2020-06-17  |Texas|99468   |3493        |2080  |23        |
            |2020-06-16  |Texas|95975   |4247        |2057  |57        |
            |2020-06-15  |Texas|91728   |2185        |2000  |15        |
            |2020-06-14  |Texas|89543   |1422        |1985  |12        |
            |2020-06-13  |Texas|88121   |2089        |1973  |21        |
            |2020-06-12  |Texas|86032   |2259        |1952  |21        |
            |2020-06-11  |Texas|83773   |2001        |1931  |31        |
            |2020-06-10  |Texas|81772   |2500        |1900  |31        |
            |2020-06-09  |Texas|79272   |1945        |1869  |13        |
            |2020-06-08  |Texas|77327   |1378        |1856  |18        |
            |2020-06-07  |Texas|75949   |15294       |1838  |138       |
            |2020-06-06  |Texas|60655   |1271        |1700  |22        |
            |2020-06-05  |Texas|59384   |1657        |1678  |22        |
            |2020-06-04  |Texas|57727   |1449        |1656  |27        |
            |2020-06-03  |Texas|56278   |1339        |1629  |32        |
            |2020-06-02  |Texas|54939   |1522        |1597  |28        |
            |2020-06-01  |Texas|53417   |-11557      |1569  |-114      |
            |2020-05-31  |Texas|64974   |13798       |1683  |148       |
            |2020-05-30  |Texas|51176   |-10897      |1535  |-110      |
            |2020-05-29  |Texas|62073   |1253        |1645  |36        |
            |2020-05-28  |Texas|60820   |1715        |1609  |29        |
            |2020-05-27  |Texas|59105   |1532        |1580  |28        |
            |2020-05-26  |Texas|57573   |1083        |1552  |21        |
            +------------+-----+--------+------------+------+----------+
            only showing top 25 rows
                
            +------------+-------+--------+------------+------+----------+
            |last_updated|state  |infected|dod_infected|deaths|dod_deaths|
            +------------+-------+--------+------------+------+----------+
            |2020-06-19  |Georgia|58231   |1338        |2615  |31        |
            |2020-06-18  |Georgia|56893   |830         |2584  |30        |
            |2020-06-17  |Georgia|56063   |1231        |2554  |64        |
            |2020-06-16  |Georgia|54832   |878         |2490  |17        |
            |2020-06-15  |Georgia|53954   |755         |2473  |41        |
            |2020-06-14  |Georgia|53199   |318         |2432  |5         |
            |2020-06-13  |Georgia|52881   |701         |2427  |27        |
            |2020-06-12  |Georgia|52180   |867         |2400  |45        |
            |2020-06-11  |Georgia|51313   |1008        |2355  |45        |
            |2020-06-10  |Georgia|50305   |827         |2310  |43        |
            |2020-06-09  |Georgia|49478   |805         |2267  |93        |
            |2020-06-08  |Georgia|48673   |705         |2174  |11        |
            |2020-06-07  |Georgia|47968   |152         |2163  |4         |
            |2020-06-06  |Georgia|47816   |579         |2159  |2         |
            |2020-06-05  |Georgia|47237   |905         |2157  |26        |
            |2020-06-04  |Georgia|46332   |959         |2131  |24        |
            |2020-06-03  |Georgia|45373   |871         |2107  |21        |
            |2020-06-02  |Georgia|44502   |256         |2086  |21        |
            |2020-06-01  |Georgia|44246   |893         |2065  |47        |
            |2020-05-31  |Georgia|43353   |219         |2018  |30        |
            |2020-05-30  |Georgia|43134   |271         |1988  |35        |
            |2020-05-29  |Georgia|42863   |819         |1953  |1         |
            |2020-05-28  |Georgia|42044   |703         |1952  |47        |
            |2020-05-27  |Georgia|41341   |1065        |1905  |38        |
            |2020-05-26  |Georgia|40276   |1156        |1867  |42        |
            +------------+-------+--------+------------+------+----------+
            only showing top 25 rows