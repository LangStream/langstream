from sga_runtime.record import Record

class TestClass(object):
  def init(self, config):
      print('init', config)
      self.secret_value = config["secret_value"]

  def process(self, records):
    processed_records = []
    for record in records:
      processed_records.append(Record(record.value() + "!!" + self.secret_value))
    return processed_records
