from sga_runtime.simplerecord import SimpleRecord

# Example Python processor that adds an exclamation mark to the end of the record value
class Exclamation(object):
  def process(self, records):
    processed_records = []
    for record in records:
      processed_records.append((record, [SimpleRecord(record.value() + "!!")]))
    return processed_records
