exports.kinesisRecord = (payload) => ({
  kinesis: {
    data: Buffer.from(JSON.stringify(payload)).toString('base64')
  }
});

exports.scheduledEvent = (options = {}) => ({
  'detail-type': 'Scheduled Event',
  resources: options.schedule ? [ options.schedule ] : [],
  source: 'aws.events',
  time: options.time || new Date().toISOString()
});

exports.streamEvent = (events) => ({
  Records: [].concat(events)
});
