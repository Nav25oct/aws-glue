## Word count using AWS Glue ( pySpark ) 

To run this code - I have created AWG Glue Dev endpoint 

ref - https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/3328674740105987/4033840715400609/6441317451288404/latest.html


```pyspark
## Import Glue and other pyspark Libraries

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import *
```


```pyspark
## Create Glue Context


glueContext = GlueContext(sc)

```


```pyspark
## Read from Glue Data Catalog

chatbot_logDyF = glueContext.create_dynamic_frame.from_catalog(database = "c360_analytics", table_name = "chatbot_log", transformation_ctx = "DataSource0")
```


```pyspark
## Printing the schema

chatbot_logDyF.printSchema()
```


```pyspark
## Converting Glue Dynamic Fram to pySpark Data Frame 

chatbot_logDF = chatbot_logDyF.toDF()
```


```pyspark
chatbot_logDF.select(col('transcript')).show(1, truncate = False)
```


```pyspark
def removePunctuation(column):
    """Removes punctuation, changes to lower case, and strips leading and trailing spaces.

    Note:
        Only spaces, letters, and numbers should be retained.  Other characters should should be
        eliminated (e.g. it's becomes its).  Leading and trailing spaces should be removed after
        punctuation is removed.

    Args:
        column (Column): A Column containing a sentence.

    Returns:
        Column: A Column named 'sentence' with clean-up operations applied.
    """
    return trim(lower(regexp_replace(column, '([^\s\w_]|_)+', ''))).alias('sentence')
```


```pyspark
chatbot_logDF = chatbot_logDF.select(removePunctuation(col('transcript')))
```


```pyspark
chatbot_logSplitDF = (chatbot_logDF
                    .select(split(chatbot_logDF.sentence, '\s+').alias('split')))
```


```pyspark
chatbot_logSplitDF.show(truncate = False)
```


```pyspark
chatbot_logSingleDF = (chatbot_logSplitDF
                    .select(explode(chatbot_logSplitDF.split).alias('chat_bot_words')))
```


```pyspark
chatbot_logSingleDF.show(15,truncate = False)
```


```pyspark
chatbot_logWordCount = chatbot_logSingleDF.count()
```


```pyspark
print(chatbot_logWordCount)
```


```pyspark

## provide your data frame name from above in group by function 
def wordCount(wordListDF):
    """Creates a DataFrame with word counts.

    Args:
        wordListDF (DataFrame of str): A DataFrame consisting of one string column called 'word'.

    Returns:
        DataFrame of (str, int): A DataFrame containing 'word' and 'count' columns.
    """
    return wordListDF.groupBy('chat_bot_words').count()
```


```pyspark
ChatWordsAndCountsDF = wordCount(chatbot_logSingleDF)
topWordsAndCountsDF = ChatWordsAndCountsDF.orderBy("count", ascending=0)

topWordsAndCountsDF.show()
```


```pyspark
### write to S3 bucket as parquet file

topWordsAndCountsDF.write.parquet('s3://navnis-idl-project/chatbot_log/chatbot_wordcount')
```
