const test = require('ava');

const { DateTime } = require('luxon');
const { generators } = require('..');

test('generating a synthetic kinesis event encodes the payload object', (test) => {
  const payload = { message: 'hello' };
  const encoded = generators.kinesisRecord(payload);

  test.deepEqual(
    JSON.parse(Buffer.from(encoded.kinesis.data, 'base64').toString('utf8')),
    payload
  );
});

test('generating a stream event from a single payload creates a single record', (test) => {
  const payload = { message: 'hello' };
  const event = generators.streamEvent(payload);
  test.deepEqual(event, { Records: [ payload ] });
});

test('generating a stream event from multiple payloads creates a record for each payload', (test) => {
  const payloads = [
    { message: 'hello' },
    { message: 'world' }
  ];

  const event = generators.streamEvent(payloads);
  test.deepEqual(event, { Records: payloads });
});

test('generating a scheduled event uses the current time and no schedule resource by default', (test) => {
  const start = DateTime.utc();
  const event = generators.scheduledEvent();
  const time = DateTime.fromISO(event.time);

  const end = DateTime.utc();
  test.is(event['detail-type'], 'Scheduled Event');
  test.is(event.source, 'aws.events');
  test.true(time.isValid);
  test.true(time >= start);
  test.true(time <= end);
  test.deepEqual(event.resources, []);
});

test('scheduled event time can be customized', (test) => {
  const time = DateTime.utc().toISO();
  const event = generators.scheduledEvent({ time });

  test.is(event['detail-type'], 'Scheduled Event');
  test.is(event.source, 'aws.events');
  test.is(event.time, time);
  test.deepEqual(event.resources, []);
});

test('scheduled event resource can be customized', (test) => {
  const schedule = 'arn:aws:events:us-east-1:123456789012:rule/my-schedule';
  const start = DateTime.utc();
  const event = generators.scheduledEvent({ schedule });
  const time = DateTime.fromISO(event.time);

  const end = DateTime.utc();
  test.is(event['detail-type'], 'Scheduled Event');
  test.is(event.source, 'aws.events');
  test.true(time.isValid);
  test.true(time >= start);
  test.true(time <= end);
  test.deepEqual(event.resources, [ schedule ]);
});
