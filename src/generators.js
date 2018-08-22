exports.kinesisRecord = (payload) => ({
  kinesis: {
    data: Buffer.from(JSON.stringify(payload)).toString('base64')
  }
});

exports.streamEvent = (events) => ({
  Records: [].concat(events)
});
