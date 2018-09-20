const rxjs = require('rxjs');
const { filter, flatMap, map, mergeAll, share, throwIfEmpty } = require('rxjs/operators');

exports.followSchedule = (options = {}) => filter((event) => {
  let isScheduledEvent = event.source === 'aws.events' && event['detail-type'] === 'Scheduled Event';

  if (options.schedule) {
    isScheduledEvent &= event.resources.includes(options.schedule);
  }

  return isScheduledEvent;
});

exports.records = () => rxjs.pipe(
  filter((event) => event.hasOwnProperty('Records')),
  flatMap(({ Records }) => Records)
);

exports.some = (...operatorFactories) => async (event, context) => {
  const streamEvent = rxjs.of(event).pipe(share());
  return rxjs.from(operatorFactories)
    .pipe(
      map((factory) => factory(context)),
      map((operator) => streamEvent.pipe(operator)),
      mergeAll(),
      throwIfEmpty()
    )
    .toPromise();
};

exports.withContext = (generator, handler) => async (event, context) => handler(event, generator(context));
