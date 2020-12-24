```pyspark
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

```

    Starting Spark application



<table>
<tr><th>ID</th><th>YARN Application ID</th><th>Kind</th><th>State</th><th>Spark UI</th><th>Driver log</th><th>Current session?</th></tr><tr><td>3</td><td>application_1608766912735_0004</td><td>pyspark</td><td>idle</td><td><a target="_blank" href="http://ip-172-32-71-48.us-west-2.compute.internal:20888/proxy/application_1608766912735_0004/">Link</a></td><td><a target="_blank" href="http://ip-172-32-77-116.us-west-2.compute.internal:8042/node/containerlogs/container_1608766912735_0004_01_000001/livy">Link</a></td><td>✔</td></tr></table>



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    SparkSession available as 'spark'.



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…



```pyspark

glueContext = GlueContext(sc)

```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…



```pyspark
chatbot_logDyF = glueContext.create_dynamic_frame.from_catalog(database = "c360_analytics", table_name = "chatbot_log", transformation_ctx = "DataSource0")
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…



```pyspark
chatbot_logDyF.printSchema()
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    root
    |-- customer_id: string
    |-- chat_duration: int
    |-- starttime: string
    |-- endtime: string
    |-- transcript: string
    |-- survey_status: string
    |-- survey_score: string
    |-- issue_resolved: string


```pyspark
chatbot_logDF = chatbot_logDyF.toDF()
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…



```pyspark
from pyspark.sql.functions import *
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…



```pyspark
chatbot_logDF.select(col('transcript')).show(truncate = False)
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |transcript                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
    +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |'customer': 'How long will it take to process my claim?', 'bot': 'You have one outstanding claim and it is waiting for your documents', 'bot': 'We are still processing your claim. We will update you when we complete your processing', 'customer': 'want to talk to an agent', 'bot': 'Please call 800-800-8000'                                                                                                                                                            |
    |'customer': 'when will my claim be processed', 'bot': 'what is your claim id?', 'customer': 'my claim_id is 20201105', 'bot': 'Claim still in progress', 'customer': 'Do you need any additional details?', 'bot': 'We recieved all required documents.', 'customer': 'when will I get an update?', 'bot': 'We are still processing your claim. We will update you when we complete your processing', 'customer': 'want to talk to an agent', 'bot': 'Please call 800-800-8000'|
    |'customer': 'what is the ETA of my claim?', 'bot': 'what is your claim id?', 'customer': 'my claim_id is 20201120', 'bot': 'Claim still in progress', 'customer': 'when will it resolved?', 'bot': 'The general processing time is 10 business days', 'customer': 'Thank you!'                                                                                                                                                                                                 |
    |'customer': 'my claim status', 'bot': 'what is your claim id?', 'customer': 'my claim_id is 20200917', 'bot': 'Claim still in progress', 'customer': 'when will it resolved?', 'bot': 'Date not available', 'customer': 'want to talk to an agent', 'bot': 'Please call 800-800-8000'                                                                                                                                                                                          |
    |'customer': 'when can I expect the claim to go through?', 'bot': 'what is your claim id?', 'customer': 'my claim_id is 20201005', 'bot': 'Claim still is declined', 'customer': 'Why is it declined?', 'bot': 'Please contact our customer service for more details.', 'customer': 'Can I get a call back?', 'bot': 'Please contact our customer service for call back requests', 'customer': 'what is the number?', 'bot': 'Our customer service number is 800-800-8000'      |
    |'customer': 'How long will it take to process my claim?', 'bot': 'You have one outstanding claim and it is waiting for your documents', 'bot': 'We are still processing your claim. We will update you when we complete your processing', 'customer': 'want to talk to an agent', 'bot': 'Please call 800-800-8000'                                                                                                                                                            |
    |'customer': 'ETA of my claim', 'bot': 'what is your claim id?', 'customer': 'my claim_id is 20201031', 'bot': 'Claim still in progress', 'customer': 'can you provide some progress details?', 'bot': 'Please contact our customer service for more details.', 'customer': 'Can I get a call back?', 'bot': 'Please contact our customer service for call back requests', 'customer': 'what is the number?', 'bot': 'Our customer service number is 800-800-8000'              |
    |'customer': 'Can I get a call back when my claim is processed?', 'bot': 'what is your claim id?', 'customer': 'my claim_id is 20201121', 'bot': 'Claim still in progress', 'customer': 'Please call me back when it is processed', 'bot': 'Please contact our customer service for call back requests', 'customer': 'what is the number?', 'bot': 'Our customer service number is 800-800-8000'                                                                                |
    |'customer': 'ETA of my claim', 'bot': 'what is your claim id?', 'customer': 'my claim_id is 20201031', 'bot': 'Claim still in progress', 'customer': 'can you provide some progress details?', 'bot': 'Please contact our customer service for more details.', 'customer': 'Can I get a call back?', 'bot': 'Please contact our customer service for call back requests', 'customer': 'what is the number?', 'bot': 'Our customer service number is 800-800-8000'              |
    |'customer': 'when will my claim be processed', 'bot': 'what is your claim id?', 'customer': 'my claim_id is 20201105', 'bot': 'Claim still in progress', 'customer': 'Do you need any additional details?', 'bot': 'We recieved all required documents.', 'customer': 'when will I get an update?', 'bot': 'We are still processing your claim. We will update you when we complete your processing', 'customer': 'want to talk to an agent', 'bot': 'Please call 800-800-8000'|
    |'customer': 'when can I expect the claim to go through?', 'bot': 'what is your claim id?', 'customer': 'my claim_id is 20201005', 'bot': 'Claim still is declined', 'customer': 'Why is it declined?', 'bot': 'Please contact our customer service for more details.', 'customer': 'Can I get a call back?', 'bot': 'Please contact our customer service for call back requests', 'customer': 'what is the number?', 'bot': 'Our customer service number is 800-800-8000'      |
    |'customer': 'Can I get a call back when my claim is processed?', 'bot': 'what is your claim id?', 'customer': 'my claim_id is 20201121', 'bot': 'Claim still in progress', 'customer': 'Please call me back when it is processed', 'bot': 'Please contact our customer service for call back requests', 'customer': 'what is the number?', 'bot': 'Our customer service number is 800-800-8000'                                                                                |
    |'customer': 'ETA of my claim', 'bot': 'what is your claim id?', 'customer': 'my claim_id is 20201031', 'bot': 'Claim still in progress', 'customer': 'can you provide some progress details?', 'bot': 'Please contact our customer service for more details.', 'customer': 'Can I get a call back?', 'bot': 'Please contact our customer service for call back requests', 'customer': 'what is the number?', 'bot': 'Our customer service number is 800-800-8000'              |
    |'customer': 'what is the ETA of my claim?', 'bot': 'what is your claim id?', 'customer': 'my claim_id is 20201120', 'bot': 'Claim still in progress', 'customer': 'when will it resolved?', 'bot': 'The general processing time is 10 business days', 'customer': 'Thank you!'                                                                                                                                                                                                 |
    |'customer': 'what is the status of my claim?', 'bot': 'what is your claim id?', 'customer': 'my claim_id is 20201011', 'bot': 'Claim is processed on 2020-10-17. Please find more details on our website', 'customer': 'In the website it still shows as under processing.', 'customer': 'want to talk to an agent', 'bot': 'Please call 800-800-8000'                                                                                                                         |
    |'customer': 'Can I get a call back when my claim is processed?', 'bot': 'what is your claim id?', 'customer': 'my claim_id is 20201121', 'bot': 'Claim still in progress', 'customer': 'Please call me back when it is processed', 'bot': 'Please contact our customer service for call back requests', 'customer': 'what is the number?', 'bot': 'Our customer service number is 800-800-8000'                                                                                |
    |'customer': 'when will my claim be processed', 'bot': 'what is your claim id?', 'customer': 'my claim_id is 20201105', 'bot': 'Claim still in progress', 'customer': 'Do you need any additional details?', 'bot': 'We recieved all required documents.', 'customer': 'when will I get an update?', 'bot': 'We are still processing your claim. We will update you when we complete your processing', 'customer': 'want to talk to an agent', 'bot': 'Please call 800-800-8000'|
    |'customer': 'what is the ETA of my claim?', 'bot': 'what is your claim id?', 'customer': 'my claim_id is 20201120', 'bot': 'Claim still in progress', 'customer': 'when will it resolved?', 'bot': 'The general processing time is 10 business days', 'customer': 'Thank you!'                                                                                                                                                                                                 |
    |'customer': 'when can I expect the claim to go through?', 'bot': 'what is your claim id?', 'customer': 'my claim_id is 20201005', 'bot': 'Claim still is declined', 'customer': 'Why is it declined?', 'bot': 'Please contact our customer service for more details.', 'customer': 'Can I get a call back?', 'bot': 'Please contact our customer service for call back requests', 'customer': 'what is the number?', 'bot': 'Our customer service number is 800-800-8000'      |
    |'customer': 'How long will it take to process my claim?', 'bot': 'You have one outstanding claim and it is waiting for your documents', 'bot': 'We are still processing your claim. We will update you when we complete your processing', 'customer': 'want to talk to an agent', 'bot': 'Please call 800-800-8000'                                                                                                                                                            |
    +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    only showing top 20 rows


```pyspark

from pyspark.sql.functions import *
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


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…



```pyspark
chatbot_logDF = chatbot_logDF.select(removePunctuation(col('transcript')))
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…



```pyspark
chatbot_logSplitDF = (chatbot_logDF
                    .select(split(chatbot_logDF.sentence, '\s+').alias('split')))
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…



```pyspark
chatbot_logSplitDF.show(truncate = False)
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |split                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
    +-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |[customer, how, long, will, it, take, to, process, my, claim, bot, you, have, one, outstanding, claim, and, it, is, waiting, for, your, documents, bot, we, are, still, processing, your, claim, we, will, update, you, when, we, complete, your, processing, customer, want, to, talk, to, an, agent, bot, please, call, 8008008000]                                                                                                                                              |
    |[customer, when, will, my, claim, be, processed, bot, what, is, your, claim, id, customer, my, claimid, is, 20201105, bot, claim, still, in, progress, customer, do, you, need, any, additional, details, bot, we, recieved, all, required, documents, customer, when, will, i, get, an, update, bot, we, are, still, processing, your, claim, we, will, update, you, when, we, complete, your, processing, customer, want, to, talk, to, an, agent, bot, please, call, 8008008000]|
    |[customer, what, is, the, eta, of, my, claim, bot, what, is, your, claim, id, customer, my, claimid, is, 20201120, bot, claim, still, in, progress, customer, when, will, it, resolved, bot, the, general, processing, time, is, 10, business, days, customer, thank, you]                                                                                                                                                                                                         |
    |[customer, my, claim, status, bot, what, is, your, claim, id, customer, my, claimid, is, 20200917, bot, claim, still, in, progress, customer, when, will, it, resolved, bot, date, not, available, customer, want, to, talk, to, an, agent, bot, please, call, 8008008000]                                                                                                                                                                                                         |
    |[customer, when, can, i, expect, the, claim, to, go, through, bot, what, is, your, claim, id, customer, my, claimid, is, 20201005, bot, claim, still, is, declined, customer, why, is, it, declined, bot, please, contact, our, customer, service, for, more, details, customer, can, i, get, a, call, back, bot, please, contact, our, customer, service, for, call, back, requests, customer, what, is, the, number, bot, our, customer, service, number, is, 8008008000]        |
    |[customer, how, long, will, it, take, to, process, my, claim, bot, you, have, one, outstanding, claim, and, it, is, waiting, for, your, documents, bot, we, are, still, processing, your, claim, we, will, update, you, when, we, complete, your, processing, customer, want, to, talk, to, an, agent, bot, please, call, 8008008000]                                                                                                                                              |
    |[customer, eta, of, my, claim, bot, what, is, your, claim, id, customer, my, claimid, is, 20201031, bot, claim, still, in, progress, customer, can, you, provide, some, progress, details, bot, please, contact, our, customer, service, for, more, details, customer, can, i, get, a, call, back, bot, please, contact, our, customer, service, for, call, back, requests, customer, what, is, the, number, bot, our, customer, service, number, is, 8008008000]                  |
    |[customer, can, i, get, a, call, back, when, my, claim, is, processed, bot, what, is, your, claim, id, customer, my, claimid, is, 20201121, bot, claim, still, in, progress, customer, please, call, me, back, when, it, is, processed, bot, please, contact, our, customer, service, for, call, back, requests, customer, what, is, the, number, bot, our, customer, service, number, is, 8008008000]                                                                             |
    |[customer, eta, of, my, claim, bot, what, is, your, claim, id, customer, my, claimid, is, 20201031, bot, claim, still, in, progress, customer, can, you, provide, some, progress, details, bot, please, contact, our, customer, service, for, more, details, customer, can, i, get, a, call, back, bot, please, contact, our, customer, service, for, call, back, requests, customer, what, is, the, number, bot, our, customer, service, number, is, 8008008000]                  |
    |[customer, when, will, my, claim, be, processed, bot, what, is, your, claim, id, customer, my, claimid, is, 20201105, bot, claim, still, in, progress, customer, do, you, need, any, additional, details, bot, we, recieved, all, required, documents, customer, when, will, i, get, an, update, bot, we, are, still, processing, your, claim, we, will, update, you, when, we, complete, your, processing, customer, want, to, talk, to, an, agent, bot, please, call, 8008008000]|
    |[customer, when, can, i, expect, the, claim, to, go, through, bot, what, is, your, claim, id, customer, my, claimid, is, 20201005, bot, claim, still, is, declined, customer, why, is, it, declined, bot, please, contact, our, customer, service, for, more, details, customer, can, i, get, a, call, back, bot, please, contact, our, customer, service, for, call, back, requests, customer, what, is, the, number, bot, our, customer, service, number, is, 8008008000]        |
    |[customer, can, i, get, a, call, back, when, my, claim, is, processed, bot, what, is, your, claim, id, customer, my, claimid, is, 20201121, bot, claim, still, in, progress, customer, please, call, me, back, when, it, is, processed, bot, please, contact, our, customer, service, for, call, back, requests, customer, what, is, the, number, bot, our, customer, service, number, is, 8008008000]                                                                             |
    |[customer, eta, of, my, claim, bot, what, is, your, claim, id, customer, my, claimid, is, 20201031, bot, claim, still, in, progress, customer, can, you, provide, some, progress, details, bot, please, contact, our, customer, service, for, more, details, customer, can, i, get, a, call, back, bot, please, contact, our, customer, service, for, call, back, requests, customer, what, is, the, number, bot, our, customer, service, number, is, 8008008000]                  |
    |[customer, what, is, the, eta, of, my, claim, bot, what, is, your, claim, id, customer, my, claimid, is, 20201120, bot, claim, still, in, progress, customer, when, will, it, resolved, bot, the, general, processing, time, is, 10, business, days, customer, thank, you]                                                                                                                                                                                                         |
    |[customer, what, is, the, status, of, my, claim, bot, what, is, your, claim, id, customer, my, claimid, is, 20201011, bot, claim, is, processed, on, 20201017, please, find, more, details, on, our, website, customer, in, the, website, it, still, shows, as, under, processing, customer, want, to, talk, to, an, agent, bot, please, call, 8008008000]                                                                                                                         |
    |[customer, can, i, get, a, call, back, when, my, claim, is, processed, bot, what, is, your, claim, id, customer, my, claimid, is, 20201121, bot, claim, still, in, progress, customer, please, call, me, back, when, it, is, processed, bot, please, contact, our, customer, service, for, call, back, requests, customer, what, is, the, number, bot, our, customer, service, number, is, 8008008000]                                                                             |
    |[customer, when, will, my, claim, be, processed, bot, what, is, your, claim, id, customer, my, claimid, is, 20201105, bot, claim, still, in, progress, customer, do, you, need, any, additional, details, bot, we, recieved, all, required, documents, customer, when, will, i, get, an, update, bot, we, are, still, processing, your, claim, we, will, update, you, when, we, complete, your, processing, customer, want, to, talk, to, an, agent, bot, please, call, 8008008000]|
    |[customer, what, is, the, eta, of, my, claim, bot, what, is, your, claim, id, customer, my, claimid, is, 20201120, bot, claim, still, in, progress, customer, when, will, it, resolved, bot, the, general, processing, time, is, 10, business, days, customer, thank, you]                                                                                                                                                                                                         |
    |[customer, when, can, i, expect, the, claim, to, go, through, bot, what, is, your, claim, id, customer, my, claimid, is, 20201005, bot, claim, still, is, declined, customer, why, is, it, declined, bot, please, contact, our, customer, service, for, more, details, customer, can, i, get, a, call, back, bot, please, contact, our, customer, service, for, call, back, requests, customer, what, is, the, number, bot, our, customer, service, number, is, 8008008000]        |
    |[customer, how, long, will, it, take, to, process, my, claim, bot, you, have, one, outstanding, claim, and, it, is, waiting, for, your, documents, bot, we, are, still, processing, your, claim, we, will, update, you, when, we, complete, your, processing, customer, want, to, talk, to, an, agent, bot, please, call, 8008008000]                                                                                                                                              |
    +-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    only showing top 20 rows


```pyspark
chatbot_logSingleDF = (chatbot_logSplitDF
                    .select(explode(chatbot_logSplitDF.split).alias('chat_bot_words')))
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…



```pyspark
chatbot_logSingleDF.show(15,truncate = False)
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +--------------+
    |chat_bot_words|
    +--------------+
    |customer      |
    |how           |
    |long          |
    |will          |
    |it            |
    |take          |
    |to            |
    |process       |
    |my            |
    |claim         |
    |bot           |
    |you           |
    |have          |
    |one           |
    |outstanding   |
    +--------------+
    only showing top 15 rows


```pyspark
chatbot_logWordCount = chatbot_logSingleDF.count()
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…



```pyspark
print(chatbot_logWordCount)
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    2491


```pyspark
# TODO: Replace <FILL IN> with appropriate code
def wordCount(wordListDF):
    """Creates a DataFrame with word counts.

    Args:
        wordListDF (DataFrame of str): A DataFrame consisting of one string column called 'word'.

    Returns:
        DataFrame of (str, int): A DataFrame containing 'word' and 'count' columns.
    """
    return wordListDF.groupBy('chat_bot_words').count()
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…



```pyspark
ChatWordsAndCountsDF = wordCount(chatbot_logSingleDF)
topWordsAndCountsDF = ChatWordsAndCountsDF.orderBy("count", ascending=0)

topWordsAndCountsDF.show()
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +--------------+-----+
    |chat_bot_words|count|
    +--------------+-----+
    |      customer|  238|
    |           bot|  177|
    |            is|  170|
    |         claim|  135|
    |            my|   77|
    |          what|   70|
    |          call|   65|
    |          your|   63|
    |        please|   60|
    |           our|   58|
    |       service|   55|
    |          when|   52|
    |          back|   50|
    |         still|   49|
    |           the|   46|
    |        number|   42|
    |      progress|   39|
    |       claimid|   39|
    |            id|   39|
    |            to|   39|
    +--------------+-----+
    only showing top 20 rows


```pyspark
topWordsAndCountsDF.write.parquet('s3://navnis-idl-project/chatbot_log/chatbot_wordcount')
```


    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…



```pyspark

```
