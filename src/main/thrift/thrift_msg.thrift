namespace java com.ociweb.protocoltest.thrift

struct ThriftSample {
  1: optional i32 id;
  2: optional i64 time;
  3: optional i32 measurement;
  4: optional i32 action;
}

struct ThriftQuery {
  1: optional i32 user;
  2: optional i32 year;
  3: optional i32 month;
  4: optional i32 date;
  5: optional i32 sample_count;

  6: list<ThriftSample> samples;
}
