from sga_runtime.record import Record
import openai
import json
from openai.embeddings_utils import get_embedding

class Embedding(object):

  def init(self, config):
    print('init', config)
    openai.api_key = config["openaiKey"]

  def process(self, records):
    processed_records = []
    for record in records:
      embedding = get_embedding(record.value(), engine='text-embedding-ada-002')
      result = {"input": str(record.value()), "embedding": embedding}
      new_value = json.dumps(result)
      processed_records.append(Record(value=new_value))
    return processed_records
