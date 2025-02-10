import argparse
from pyspark.sql import SparkSession
from dataclasses import dataclass
import json

@dataclass
class Tweet:
  """Class to model a Tweet"""
  id: int         # The unique ID of a tweet
  content: str    # The textual content of a tweet
  author: str     # The nickname of the author of the tweet
  language: str   # The language of the tweet

def toTweet(line: str):
  try:
    parsed = json.loads(line)
    return Tweet(parsed['id'], parsed['text'], parsed['user']['name'], parsed['lang'])
  except Exception as e:
    return None

def count_by_language(input_uri, output_uri):
  spark = (
      SparkSession
      .builder
      .appName("tweets_by_language")
      .getOrCreate()
    )
  sc = spark.sparkContext
  rdd = sc.textFile(input_uri)
  processed = (rdd
      .map(toTweet)
      .filter(lambda x: x is not None)
  )
  by_lang = (
  processed
      .map(lambda x: (x.language, 1)) #from rdd[Tweet] to rdd[str, int]
      .reduceByKey(lambda x, y: x + y)
      .sortBy(lambda x: x[1], ascending=False)
  )
  by_lang.saveAsTextFile(output_uri)
  
if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--data_source', help="The URI for you data, like an S3 bucket location.")
  parser.add_argument(
      '--output_uri', help="The URI where output is saved, like an S3 bucket location.")
  args = parser.parse_args()

  count_by_language(args.data_source, args.output_uri)
