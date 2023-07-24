from sga_runtime.record import Record

# Example Python processor that adds an exclamation mark to the end of the record value
class Exclamation(object):
  def process(self, records):
    processed_records = []
    for record in records:
      processed_records.append(Record(record.value() + "!!"))
    return processed_records
