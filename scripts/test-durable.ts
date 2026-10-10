export {};
if (
  !process.env.TEST_DATABASE_URL ||
  !process.env.OPENBOT_DURABLE_TEST_DATABASE_URL
) {
  console.error(
    "TEST_DATABASE_URL and OPENBOT_DURABLE_TEST_DATABASE_URL must name dedicated test databases. Run server/scripts/migrate.ts against TEST_DATABASE_URL first.",
  );
  process.exit(1);
}
const child = Bun.spawn(
  [
    "bun",
    "test",
    "packages/durable-core/tests",
    "server/tests/local-threads-http.test.ts",
    "server/tests/local-headless.integration.test.ts",
    "server/tests/local-thread-policy.integration.test.ts",
    "server/tests/local-startup.integration.test.ts",
    "server/tests/local-user-path.integration.test.ts",
  ],
  { stdout: "inherit", stderr: "inherit" },
);
process.exitCode = await child.exited;
