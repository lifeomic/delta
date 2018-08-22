const test = require('ava');
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
