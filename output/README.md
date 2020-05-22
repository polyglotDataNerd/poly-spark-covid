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
    
Output Sample Query Result Sets
-
* Refer to compress gzip in the directory
        
       Corona Data Scraper    
        +------------+-----------+
		|last_updated|cds_records|
		+------------+-----------+
		|2020-05-01  |3165       |
		|2020-04-30  |3154       |
		|2020-04-29  |3156       |
		|2020-04-28  |3145       |
		|2020-04-27  |3135       |
		+------------+-----------+
		only showing top 5 rows
        
       Johns Hopkins University
		+------------+-----------+
		|last_updated|jhu_records|
		+------------+-----------+
		|2020-05-22  |2995       |
		|2020-05-21  |2993       |
		|2020-05-20  |2987       |
		|2020-05-19  |2982       |
		|2020-05-18  |2981       |
		+------------+-----------+
		only showing top 5 rows
        
       Deaths
		+-----------+---------+-----------+
		|last_updated|us_deaths|us_affected|
		+-----------+---------+-----------+
		|2020-05-22 |94,702   |1,577,147  |
		|2020-05-21 |93,439   |1,551,853  |
		|2020-05-20 |91,921   |1,528,568  |
		|2020-05-19 |90,347   |1,508,308  |
		|2020-05-18 |89,562   |1,486,757  |
		|2020-05-17 |88,754   |1,467,820  |
		|2020-05-16 |87,530   |1,442,824  |
		|2020-05-15 |85,898   |1,417,774  |
		|2020-05-14 |84,119   |1,390,406  |
		|2020-05-13 |82,376   |1,369,574  |
		|2020-05-12 |80,682   |1,347,881  |
		|2020-05-11 |79,526   |1,329,260  |
		|2020-05-10 |78,795   |1,309,550  |
		+-----------+---------+-----------+
		only showing top 15 rows
        
       Consolidated
		+------------+----------------+
		|last_updated|combined_records|
		+------------+----------------+
		|2020-05-21  |3078            |
		|2020-05-20  |3077            |
		|2020-05-19  |3074            |
		|2020-05-18  |3068            |
		|2020-05-17  |3066            |
		+------------+----------------+
		only showing top 5 rows

       USA
		+------------+--------------+--------+------+---------+------------+----------+
		|last_updated|state         |infected|deaths|recovered|hospitalized|discharged|
		+------------+--------------+--------+------+---------+------------+----------+
		|2020-05-21  |New York      |356,458 |7,700 |null     |null        |null      |
		|2020-05-21  |New Jersey    |150,446 |10,843|null     |null        |null      |
		|2020-05-21  |Illinois      |102,512 |4,606 |null     |null        |null      |
		|2020-05-21  |Massachusetts |89,780  |6,145 |null     |null        |null      |
		|2020-05-21  |California    |88,323  |3,599 |11,628   |7,847       |null      |
		|2020-05-21  |Pennsylvania  |65,392  |4,869 |null     |null        |null      |
		|2020-05-21  |Texas         |52,183  |1,439 |null     |null        |null      |
		|2020-05-21  |Michigan      |50,074  |5,064 |null     |null        |null      |
		|2020-05-21  |Florida       |48,593  |2,144 |null     |null        |null      |
		|2020-05-21  |Maryland      |42,434  |2,049 |null     |null        |null      |
		|2020-05-21  |Connecticut   |39,014  |3,580 |null     |null        |null      |
		|2020-05-21  |Georgia       |36,505  |1,747 |null     |7,126       |null      |
		|2020-05-21  |Louisiana     |36,426  |2,506 |null     |null        |null      |
		|2020-05-21  |Virginia      |32,909  |1,074 |null     |null        |null      |
		|2020-05-21  |Ohio          |30,167  |1,837 |null     |null        |null      |
		|2020-05-21  |Colorado      |23,100  |1,309 |null     |null        |null      |
		|2020-05-21  |North Carolina|20,859  |716   |null     |null        |null      |
		|2020-05-21  |Washington    |19,051  |1,044 |null     |null        |null      |
		|2020-05-21  |Tennessee     |18,425  |305   |null     |null        |null      |
		|2020-05-21  |Iowa          |15,946  |400   |null     |null        |null      |
		|2020-05-21  |Arizona       |15,315  |745   |null     |null        |null      |
		|2020-05-21  |Wisconsin     |13,885  |487   |null     |null        |null      |
		|2020-05-21  |Alabama       |13,414  |529   |null     |null        |null      |
		|2020-05-21  |Minnesota     |12,464  |760   |null     |null        |null      |
		|2020-05-21  |Indiana       |12,438  |691   |null     |null        |null      |
		|2020-05-21  |Mississippi   |12,222  |580   |null     |null        |null      |
		|2020-05-21  |Rhode Island  |11,800  |425   |null     |null        |null      |
		|2020-05-21  |Nebraska      |11,253  |141   |null     |626         |null      |
		|2020-05-21  |South Carolina|9,379   |416   |null     |null        |null      |
		|2020-05-21  |Utah          |9,190   |20    |null     |null        |null      |
		|2020-05-21  |Kansas        |8,539   |202   |null     |null        |null      |
		|2020-05-21  |Kentucky      |8,450   |393   |4,289    |null        |null      |
		|2020-05-21  |Nevada        |7,313   |372   |780      |58          |111       |
		|2020-05-21  |Missouri      |7,015   |287   |2,017    |null        |null      |
		|2020-05-21  |New Mexico    |6,317   |283   |null     |null        |null      |
		|2020-05-21  |Delaware      |5,909   |202   |null     |null        |null      |
		|2020-05-21  |Oklahoma      |5,561   |301   |4,265    |null        |null      |
		|2020-05-21  |Arkansas      |5,307   |110   |3,802    |null        |null      |
		|2020-05-21  |South Dakota  |4,177   |46    |null     |null        |null      |
		|2020-05-21  |New Hampshire |3,854   |190   |null     |null        |null      |
		|2020-05-21  |Oregon        |3,817   |145   |null     |null        |null      |
		|2020-05-21  |Idaho         |2,506   |77    |null     |null        |null      |
		|2020-05-21  |North Dakota  |2,229   |40    |null     |null        |null      |
		|2020-05-21  |Maine         |1,874   |73    |1,145    |null        |null      |
		|2020-05-21  |West Virginia |1,567   |69    |null     |null        |null      |
		|2020-05-21  |Vermont       |945     |54    |null     |null        |null      |
		|2020-05-21  |Wyoming       |801     |1     |null     |null        |null      |
		|2020-05-21  |Hawaii        |637     |17    |null     |null        |null      |
		|2020-05-21  |Alaska        |401     |9     |null     |null        |null      |
		|2020-05-21  |Montana       |332     |16    |null     |null        |null      |
		+------------+--------------+--------+------+---------+------------+----------+