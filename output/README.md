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

 1. [Deaths and affected by COVID by day and state](https://github.com/polyglotDataNerd/poly-spark-covid/blob/master/src/main/scala/com/poly/covid/sql/Insights.scala#L45): this is an exact match John Hopkins Outputs in their daily graph. 
    
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
        |2020-06-19  |California    |170727  |10844 |31042    |2540        |null      |
        |2020-06-19  |New Jersey    |167803  |25670 |null     |null        |null      |
        |2020-06-19  |Illinois      |135417  |13160 |null     |null        |null      |
        |2020-06-19  |Massachusetts |106354  |15590 |null     |null        |null      |
        |2020-06-19  |Texas         |102680  |4277  |null     |null        |null      |
        |2020-06-19  |Florida       |89635   |6208  |null     |null        |null      |
        |2020-06-19  |Pennsylvania  |84780   |12816 |null     |null        |null      |
        |2020-06-19  |Michigan      |66825   |12129 |null     |null        |null      |
        |2020-06-19  |Maryland      |63839   |6020  |null     |null        |null      |
        |2020-06-19  |Georgia       |58231   |5230  |null     |9580        |null      |
        |2020-06-19  |Virginia      |56240   |3188  |null     |null        |null      |
        |2020-06-19  |North Carolina|49686   |2394  |null     |null        |null      |
        |2020-06-19  |Louisiana     |48176   |6034  |null     |null        |null      |
        |2020-06-19  |Arizona       |46689   |1312  |null     |null        |null      |
        |2020-06-19  |Ohio          |43731   |5334  |null     |null        |null      |
        |2020-06-19  |Connecticut   |43346   |8476  |null     |null        |null      |
        |2020-06-19  |Tennessee     |32604   |1019  |null     |null        |null      |
        |2020-06-19  |Minnesota     |31617   |2736  |null     |null        |null      |
        |2020-06-19  |Colorado      |30172   |3286  |null     |null        |null      |
        |2020-06-19  |Alabama       |28583   |1634  |null     |null        |null      |
        |2020-06-19  |Washington    |27576   |2500  |null     |null        |null      |
        |2020-06-19  |Iowa          |25156   |1361  |15683    |null        |null      |
        |2020-06-19  |Wisconsin     |24154   |1460  |null     |null        |null      |
        |2020-06-19  |South Carolina|21548   |1260  |null     |null        |null      |
        |2020-06-19  |Mississippi   |20641   |1876  |null     |null        |null      |
        |2020-06-19  |Utah          |20466   |166   |null     |null        |null      |
        |2020-06-19  |Nebraska      |17411   |486   |null     |1180        |null      |
        |2020-06-19  |Rhode Island  |14524   |1691  |null     |null        |null      |
        |2020-06-19  |Arkansas      |14098   |428   |9506     |null        |null      |
        |2020-06-19  |Kentucky      |13352   |1092  |9445     |null        |null      |
        |2020-06-19  |Indiana       |12438   |1322  |null     |null        |null      |
        |2020-06-19  |Nevada        |12196   |954   |1422     |null        |174       |
        |2020-06-19  |Kansas        |12059   |254   |null     |null        |null      |
        |2020-06-19  |Missouri      |11680   |1353  |2817     |null        |null      |
        |2020-06-19  |New Mexico    |10153   |912   |null     |null        |null      |
        |2020-06-19  |Oklahoma      |9358    |727   |6985     |null        |null      |
        |2020-06-19  |Oregon        |6572    |376   |null     |null        |null      |
        |2020-06-19  |South Dakota  |6109    |159   |null     |null        |null      |
        |2020-06-19  |Delaware      |5909    |635   |null     |null        |null      |
        |2020-06-19  |New Hampshire |5449    |662   |null     |null        |null      |
        |2020-06-19  |Idaho         |3743    |178   |null     |null        |null      |
        |2020-06-19  |North Dakota  |3193    |157   |null     |null        |null      |
        |2020-06-19  |Maine         |2876    |204   |null     |null        |null      |
        |2020-06-19  |West Virginia |2418    |176   |null     |null        |null      |
        |2020-06-19  |Wyoming       |1173    |20    |null     |null        |null      |
        |2020-06-19  |Vermont       |1139    |112   |null     |null        |null      |
        |2020-06-19  |Alaska        |793     |22    |null     |null        |null      |
        |2020-06-19  |Hawaii        |777     |null  |null     |null        |null      |
        |2020-06-19  |Montana       |332     |20    |null     |null        |null      |
        +------------+--------------+--------+------+---------+------------+----------+

  3. [State Day over Day delta's Deaths and Affected](https://github.com/polyglotDataNerd/poly-spark-covid/blob/master/src/main/scala/com/poly/covid/sql/Insights.scala#L92)

            +------------+-------+--------+------------+------+----------+
            |last_updated|state  |infected|dod_infected|deaths|dod_deaths|
            +------------+-------+--------+------------+------+----------+
            |2020-06-19  |Florida|89635   |3819        |6208  |86        |
            |2020-06-18  |Florida|85816   |5804        |6122  |136       |
            |2020-06-17  |Florida|80012   |8           |5986  |2         |
            |2020-06-16  |Florida|80004   |2775        |5984  |108       |
            |2020-06-15  |Florida|77229   |1763        |5876  |14        |
            |2020-06-14  |Florida|75466   |2016        |5862  |12        |
            |2020-06-13  |Florida|73450   |4477        |5850  |154       |
            |2020-06-12  |Florida|68973   |0           |5696  |0         |
            |2020-06-11  |Florida|68973   |1698        |5696  |94        |
            |2020-06-10  |Florida|67275   |1368        |5602  |72        |
            |2020-06-09  |Florida|65907   |1104        |5530  |108       |
            |2020-06-08  |Florida|64803   |4710        |5422  |208       |
            |2020-06-07  |Florida|60093   |0           |5214  |0         |
            |2020-06-06  |Florida|60093   |0           |5214  |0         |
            |2020-06-05  |Florida|60093   |0           |5214  |0         |
            |2020-06-04  |Florida|60093   |1418        |5214  |82        |
            |2020-06-03  |Florida|58675   |1315        |5132  |72        |
            |2020-06-02  |Florida|57360   |614         |5060  |140       |
            |2020-06-01  |Florida|56746   |675         |4920  |20        |
            |2020-05-31  |Florida|56071   |731         |4900  |6         |
            |2020-05-30  |Florida|55340   |934         |4894  |70        |
            |2020-05-29  |Florida|54406   |1207        |4824  |96        |
            |2020-05-28  |Florida|53199   |650         |4728  |90        |
            |2020-05-27  |Florida|52549   |387         |4638  |122       |
            |2020-05-26  |Florida|52162   |501         |4516  |12        |
            +------------+-------+--------+------------+------+----------+
            only showing top 25 rows
            
            +------------+-----+--------+------------+------+----------+
            |last_updated|state|infected|dod_infected|deaths|dod_deaths|
            +------------+-----+--------+------------+------+----------+
            |2020-06-19  |Texas|102680  |1           |4277  |35        |
            |2020-06-18  |Texas|102679  |3211        |4242  |81        |
            |2020-06-17  |Texas|99468   |3493        |4161  |47        |
            |2020-06-16  |Texas|95975   |4247        |4114  |113       |
            |2020-06-15  |Texas|91728   |2185        |4001  |30        |
            |2020-06-14  |Texas|89543   |1422        |3971  |25        |
            |2020-06-13  |Texas|88121   |2089        |3946  |42        |
            |2020-06-12  |Texas|86032   |2259        |3904  |42        |
            |2020-06-11  |Texas|83773   |2001        |3862  |61        |
            |2020-06-10  |Texas|81772   |2500        |3801  |62        |
            |2020-06-09  |Texas|79272   |1945        |3739  |27        |
            |2020-06-08  |Texas|77327   |1378        |3712  |36        |
            |2020-06-07  |Texas|75949   |15294       |3676  |276       |
            |2020-06-06  |Texas|60655   |1271        |3400  |44        |
            |2020-06-05  |Texas|59384   |1657        |3356  |43        |
            |2020-06-04  |Texas|57727   |1449        |3313  |54        |
            |2020-06-03  |Texas|56278   |1339        |3259  |65        |
            |2020-06-02  |Texas|54939   |1522        |3194  |56        |
            |2020-06-01  |Texas|53417   |-11557      |3138  |-228      |
            |2020-05-31  |Texas|64974   |13798       |3366  |295       |
            |2020-05-30  |Texas|51176   |-10897      |3071  |-219      |
            |2020-05-29  |Texas|62073   |1253        |3290  |71        |
            |2020-05-28  |Texas|60820   |1715        |3219  |59        |
            |2020-05-27  |Texas|59105   |1532        |3160  |56        |
            |2020-05-26  |Texas|57573   |1083        |3104  |41        |
            +------------+-----+--------+------------+------+----------+
            only showing top 25 rows
            
            +------------+-------+--------+------------+------+----------+
            |last_updated|state  |infected|dod_infected|deaths|dod_deaths|
            +------------+-------+--------+------------+------+----------+
            |2020-06-19  |Georgia|58231   |1338        |5230  |61        |
            |2020-06-18  |Georgia|56893   |830         |5169  |60        |
            |2020-06-17  |Georgia|56063   |1231        |5109  |129       |
            |2020-06-16  |Georgia|54832   |878         |4980  |34        |
            |2020-06-15  |Georgia|53954   |755         |4946  |82        |
            |2020-06-14  |Georgia|53199   |318         |4864  |10        |
            |2020-06-13  |Georgia|52881   |701         |4854  |53        |
            |2020-06-12  |Georgia|52180   |867         |4801  |91        |
            |2020-06-11  |Georgia|51313   |1008        |4710  |90        |
            |2020-06-10  |Georgia|50305   |827         |4620  |86        |
            |2020-06-09  |Georgia|49478   |805         |4534  |186       |
            |2020-06-08  |Georgia|48673   |705         |4348  |22        |
            |2020-06-07  |Georgia|47968   |152         |4326  |7         |
            |2020-06-06  |Georgia|47816   |579         |4319  |4         |
            |2020-06-05  |Georgia|47237   |905         |4315  |52        |
            |2020-06-04  |Georgia|46332   |959         |4263  |48        |
            |2020-06-03  |Georgia|45373   |871         |4215  |43        |
            |2020-06-02  |Georgia|44502   |256         |4172  |41        |
            |2020-06-01  |Georgia|44246   |893         |4131  |95        |
            |2020-05-31  |Georgia|43353   |219         |4036  |60        |
            |2020-05-30  |Georgia|43134   |271         |3976  |70        |
            |2020-05-29  |Georgia|42863   |819         |3906  |2         |
            |2020-05-28  |Georgia|42044   |703         |3904  |93        |
            |2020-05-27  |Georgia|41341   |1065        |3811  |77        |
            |2020-05-26  |Georgia|40276   |1156        |3734  |84        |
            +------------+-------+--------+------------+------+----------+
            only showing top 25 rows