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
    icu:string
    
Output Sample Query Result Sets
-
* Refer to compress gzip in the directory

        CDS
        2020-05-01      3165
        2020-04-30      3154
        2020-04-29      3156
        2020-04-28      3145
        2020-04-27      3135
        
        JHU
        2020-05-03      2919
        2020-05-02      2914
        2020-05-01      2905
        2020-04-29      2893
        2020-04-28      2785
        
        US DEATHS
        2020-05-03      66,368  1,132,521
        2020-05-02      64,943  1,103,415
        2020-05-01      62,996  1,069,424
        2020-04-29      58,355  1,012,582
        2020-04-28      50,342  843,004
        
        +------------+--------------+--------+------+---------+------------+----------+
        |last_updated|state         |infected|deaths|recovered|hospitalized|discharged|
        +------------+--------------+--------+------+---------+------------+----------+
        |2020-04-16  |New York      |222,284 |3,355 |null     |null        |null      |
        |2020-04-16  |New Jersey    |69,841  |3,505 |null     |null        |null      |
        |2020-04-16  |Massachusetts |31,543  |1,093 |null     |null        |null      |
        |2020-04-16  |California    |28,035  |246   |1,753    |null        |null      |
        |2020-04-16  |Michigan      |27,310  |1,963 |null     |null        |null      |
        |2020-04-16  |Pennsylvania  |26,804  |841   |null     |null        |null      |
        |2020-04-16  |Illinois      |25,679  |1,053 |null     |null        |null      |
        |2020-04-16  |Florida       |22,526  |537   |null     |null        |null      |
        |2020-04-16  |Louisiana     |22,476  |1,118 |null     |null        |null      |
        |2020-04-16  |Texas         |16,455  |389   |null     |null        |null      |
        |2020-04-16  |Connecticut   |15,350  |746   |null     |null        |null      |
        |2020-04-16  |Georgia       |14,447  |586   |null     |null        |null      |
        |2020-04-16  |Washington    |10,818  |579   |null     |null        |null      |
        |2020-04-16  |Maryland      |10,784  |221   |null     |null        |null      |
        |2020-04-16  |Indiana       |9,542   |469   |null     |null        |null      |
        |2020-04-16  |Colorado      |8,579   |310   |null     |null        |null      |
        |2020-04-16  |Ohio          |8,414   |391   |null     |null        |null      |
        |2020-04-16  |Virginia      |6,889   |126   |null     |null        |null      |
        |2020-04-16  |Tennessee     |5,803   |135   |2,645    |null        |null      |
        |2020-04-16  |North Carolina|5,463   |149   |null     |null        |null      |
        |2020-04-16  |Missouri      |5,061   |44    |564      |null        |null      |
        |2020-04-16  |Alabama       |4,404   |133   |null     |null        |null      |
        |2020-04-16  |Arizona       |4,232   |122   |null     |null        |null      |
        |2020-04-16  |South Carolina|3,931   |111   |null     |null        |null      |
        |2020-04-16  |Wisconsin     |3,875   |194   |null     |null        |null      |
        |2020-04-16  |Mississippi   |3,624   |120   |null     |null        |null      |
        |2020-04-16  |Nevada        |3,302   |131   |115      |null        |null      |
        |2020-04-16  |Utah          |3,159   |7     |null     |null        |null      |
        |2020-04-16  |Rhode Island  |2,495   |3     |null     |null        |null      |
        |2020-04-16  |Kentucky      |2,470   |129   |425      |null        |null      |
        |2020-04-16  |Oklahoma      |2,357   |131   |null     |null        |null      |
        |2020-04-16  |Iowa          |2,141   |60    |null     |null        |null      |
        |2020-04-16  |Delaware      |2,053   |27    |null     |null        |null      |
        |2020-04-16  |Minnesota     |1,911   |81    |null     |null        |null      |
        |2020-04-16  |Oregon        |1,736   |64    |null     |null        |null      |
        |2020-04-16  |Idaho         |1,611   |23    |null     |null        |null      |
        |2020-04-16  |Kansas        |1,588   |80    |null     |null        |null      |
        |2020-04-16  |Arkansas      |1,576   |35    |548      |null        |null      |
        |2020-04-16  |New Mexico    |1,484   |25    |null     |null        |null      |
        |2020-04-16  |New Hampshire |1,210   |3     |null     |null        |null      |
        |2020-04-16  |Nebraska      |987     |15    |null     |null        |null      |
        |2020-04-16  |Maine         |794     |27    |333      |null        |null      |
        |2020-04-16  |Vermont       |759     |33    |null     |null        |null      |
        |2020-04-16  |West Virginia |725     |6     |null     |null        |null      |
        |2020-04-16  |Hawaii        |530     |9     |null     |null        |null      |
        |2020-04-16  |Wyoming       |401     |1     |null     |null        |null      |
        |2020-04-16  |North Dakota  |393     |9     |null     |null        |null      |
        |2020-04-16  |South Dakota  |373     |7     |null     |null        |null      |
        |2020-04-16  |Montana       |332     |7     |null     |null        |null      |
        |2020-04-16  |Alaska        |292     |4     |null     |null        |null      |
        +------------+--------------+--------+------+---------+------------+----------+