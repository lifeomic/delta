class UnhandledEventError extends Error {
  constructor (event) {
    const message = JSON.stringify(event, 2, null);
    super(message);
    this.name = 'UnhandledEventError';
  }
}

module.exports = {
  UnhandledEventError
};
