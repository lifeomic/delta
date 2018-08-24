delta
=====

[![Build Status](https://travis-ci.org/lifeomic/delta.svg?branch=master)](https://travis-ci.org/lifeomic/delta)
[![Coverage Status](https://coveralls.io/repos/github/lifeomic/delta/badge.svg?branch=master)](https://coveralls.io/github/lifeomic/delta?branch=master)

Delta provides tooling for creating and testing AWS Lambda functions that
process AWS data streams.

## Usage

```bash
npm install --save @lifeomic/delta
```

## API

### Handlers

The `delta.handlers` module provides tools for constructing `async` AWS Lambda
handlers using [RxJS][rxjs].

#### records()

Create an [RxJS][rxjs] operator for extracting AWS Lambda data stream records
from an event.

#### some(...operatorFactories)

Create an AWS Lambda handler from a set of `operatorFactories`. Each fatory is
invoked with the `context` object from the Lambda invocation and is expected to
return an [RxJS][rxjs] operator. The Lambda `event` is then piped through all of
of the operators and the last emitted value is returned. If no operator emits
a value an error is thrown.

#### withContext(generator, handler)

Creates a new AWS Lambda handler that wraps a given `handler`. The wrapper will
invoke the `generator` with the `context` object Lambda invocation. The
transformed `context` and the original Lambda `event` are then passed to the
`handler` and the result is retuned.

### Generators

The `delta.generators` module provides tools for generating synthetic data
stream event payloads. This is often useful for testing.

#### kinesisRecord(payload)

Encodes the `payload` object and returns a single Kinesis record.

#### streamEvent(records)

Encodes `records` as a event object like what AWS Lambda receives from a
registered data stream. `records` may  be a single record on an array of
records.

[rxjs]: https://rxjs-dev.firebaseapp.com/ "RxJS"
