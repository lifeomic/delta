module.exports = {
  extends: ['@lifeomic/standards', 'prettier', 'plugin:prettier/recommended'],
  plugins: ['prettier'],
  env: {
    node: true,
  },
  overrides: [
    {
      files: ['**/*.test.ts'],
      rules: {
        '@typescript-eslint/no-unsafe-argument': 'off',
      },
    },
  ],
};
