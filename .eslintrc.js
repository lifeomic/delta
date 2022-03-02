module.exports = {
  extends: ['prettier', '@lifeomic/standards'],
  overrides: [
    // Set correct env for config files
    { files: ['*.js'], env: { node: true } },
  ],
};
