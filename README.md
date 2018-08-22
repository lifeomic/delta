delta
=====

Delta provides tooling for creating and testing AWS Lambda functions that
process AWS data streams.

## Usage

```bash
npm install --save @lifeomic/delta
```

## API

### Handlers

The `delta.handlers` module provides tools for constructing `async` AWS Lambda
handlers. These tools work best when used in combination with a library like
[lodash][lodash] or [ramda][ramda]. Each function in this module returns a
handle function of the form `async (event, context) => . . .`

#### pipeline(...steps)

Create a handler than runs a sequence of steps until one returns `undefined`.
The first step is invoked with the received `(event, context)` arguments. Each
subsequent step is invoked with `(returnValue, context)` where `returnValue` is
the result of the previous step. If any step returns `undefined` the sequence is
aborted and returns `undefined`. If the sequence completes `pipeline()` returns
the result of the last step.

#### some(...steps)

Create a handler that runs a sequence of steps until once returns a value. Each
step is invoked with the received `(event, context)` arguments. As soon as one
step returns a value other than `undefined` the sequence terminates and returns
the result of that step. If no step returns a value then `some()` throws an
`UnhandledEventError`.

### Generators

The `delta.generators` module provides tools for generating synthetic data
stream event payloads. This is often useful for testing.

#### kinesisRecord(payload)

Encodes the `payload` object and returns a single Kinesis record.

#### streamEvent(records)

Encodes `records` as a event object like what AWS Lambda receives from a
registered data stream. `records` may  be a single record on an array of
records.

### Errors

The `delta.errors` module provides the various error types used by the rest of
the package.

[lodash]: https://lodash.com/ "lodash"
[ramda]: https://ramdajs.com/ "Ramda"
