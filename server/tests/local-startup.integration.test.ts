import { expect, test } from "bun:test";
import { fileURLToPath } from "node:url";
import { testDatabaseUrl } from "./support/database";

test("full local application starts without Intelligence construction or egress", async () => {
  const reservation = Bun.serve({
    port: 0,
    hostname: "127.0.0.1",
    fetch: () => new Response(),
  });
  const port = reservation.port;
  reservation.stop(true);
  const root = fileURLToPath(new URL("../..", import.meta.url));
  const child = Bun.spawn(
    [process.execPath, "server/tests/fixtures/local-startup.ts"],
    {
      cwd: root,
      env: {
        PATH: process.env.PATH ?? "/usr/bin:/bin",
        HOME: "/tmp",
        THREADS_BACKEND: "local",
        DATABASE_URL: testDatabaseUrl(),
        TENANT_PACKAGE_DIR: `${root}/examples/fintech`,
        KEY_ENCRYPTION_KEY: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
        OPENBOT_SINGLE_USER: "true",
        PORT: String(port),
        COPILOTKIT_TELEMETRY_DISABLED: "true",
        DO_NOT_TRACK: "1",
        OPENBOT_SELF_HOST_BANNER: "false",
        DEPLOYMENT_ID: `startup-${crypto.randomUUID()}`,
      },
      stdout: "pipe",
      stderr: "pipe",
    },
  );
  const outputPromise = Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  let capabilities: Record<string, unknown> | undefined;
  try {
    for (let i = 0; i < 80; i++) {
      await Bun.sleep(100);
      try {
        const response = await fetch(
          `http://127.0.0.1:${port}/api/capabilities`,
        );
        if (response.ok) {
          capabilities = await response.json();
          break;
        }
      } catch {}
    }
  } finally {
    child.kill("SIGTERM");
    const force = setTimeout(() => child.kill("SIGKILL"), 3000);
    await child.exited;
    clearTimeout(force);
  }
  const output = (await outputPromise).join("\n");
  expect(output).not.toContain("FORBIDDEN_");
  expect(capabilities).toMatchObject({
    mode: "local",
    learning: false,
    threadTransport: "sse",
    durableHistory: true,
  });
}, 20_000);
