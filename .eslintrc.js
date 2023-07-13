module.exports = {
  extends: ['@lifeomic/standards', 'prettier', 'plugin:prettier/recommended'],
  plugins: ['prettier'],
  overrides: [
    // Set correct env for config files
    { files: ['*.js'], env: { node: true } },
    {
      files: ['**/*.test.ts'],
      rules: {
        '@typescript-eslint/no-unsafe-argument': 'off',
      },
    },
  ],
};