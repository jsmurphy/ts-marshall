# @ts/marshall

A distributed task-lock primitive (**Marshall**) with cooperative-abort support.

Marshall lets multiple processes or services coordinate so that a named task runs on
**one host at a time**. It is backed by MongoDB and is fully dependency-injected, so the
core module contains no service-specific imports and can be shared verbatim across
services. Each consumer keeps a tiny shim that injects its own Mongo collections map and
logger.

This is especially useful in distributed environments where you want a **single instance
of a task to run across a fleet of nodes** — for example, ensuring only one replica in a
Kubernetes deployment executes a scheduled job or sync, even though every pod is running
the same code.

## Features

- **Single-runner locking** — only one process acquires the lock for a given task id.
- **Run throttling** (`mins`) — refuse to start again until a minimum interval has elapsed.
- **Timeout recovery** (`timeout`) — automatically reclaim a lock from a crashed/hung run.
- **Cooperative abort** — release locks on graceful shutdown so a restart isn't locked
  out for the full task timeout.
- **Administrative disable** — skip tasks flagged `enabled: false`.
- **Force start** — bypass throttling on demand or via a persisted `force` flag.
- **Dependency injected** — no service-specific imports; portable across repos.

## Requirements

- Node.js >= 18
- A MongoDB collection handle (the `tasks` collection by default).

## Installation

```sh
npm install @ts/marshall
```

## Usage

### 1. Create a repo-local shim

Each consuming service drops in a small shim that injects its own dependencies and
re-exports the bound class as the default export. This keeps every existing
`import Marshall from './marshall.mjs'; new Marshall(id)` call site working unchanged.

```js
// modules/.../marshall.mjs (shim)
import { coll } from './conn.mjs';
import { logger } from './logger.mjs';
import { createMarshall } from '@ts/marshall';

export default createMarshall({ coll, logger });
```

See [examples/repo-shim.mjs](examples/repo-shim.mjs) for a fuller example.

### 2. Use the lock around a task

```js
import Marshall from './marshall.mjs';

const marshall = new Marshall('nightly-sync');
marshall.mins(5);      // don't re-run within 5 minutes of the last run
marshall.timeout(20);  // reclaim the lock if a run exceeds 20 minutes

const { started, status } = await marshall.start();
if (!started) {
  // e.g. AlreadyRunning, TooSoon, Disabled, ...
  return;
}

try {
  await doWork();
  await marshall.finish({ records: 1234 }); // optional metadata persisted on the task
} catch (err) {
  await marshall.error(err);
}
```

## API

### `createMarshall({ coll, logger, hardTimeout, tasksName })`

Factory that returns a bound `Marshall` class.

| Option        | Type     | Default     | Description                                                      |
| ------------- | -------- | ----------- | ---------------------------------------------------------------- |
| `coll`        | object   | _required_  | Map of Mongo collection handles, keyed by name (accessed lazily).|
| `logger`      | object   | no-op       | Pino-style logger (`{obj}, msg`).                                |
| `hardTimeout` | number   | `20`        | Default task timeout in minutes.                                 |
| `tasksName`   | string   | `'tasks'`   | Name of the tasks collection within `coll`.                      |

### Instance methods

| Method                 | Description                                                                 |
| ---------------------- | --------------------------------------------------------------------------- |
| `new Marshall(id)`     | Create a lock for the task named `id`.                                       |
| `mins(minutes)`        | Minimum interval before the task may start again (integer >= 1).            |
| `timeout(minutes)`     | Timeout after which a held lock is considered stale and reclaimable.        |
| `start()`              | Attempt to acquire the lock. Resolves to `{ started, status, ... }`.        |
| `finish(metadata?)`    | Mark the task `Completed`, optionally persisting extra metadata.            |
| `error(error)`         | Mark the task `Error` and record the error details.                         |
| `abort()`              | Release the lock for a graceful shutdown (marks the task `Aborted`).        |
| `terminate()`          | Mark the task as terminated (does not perform termination).                 |

Setting `marshall.force = true` bypasses throttling for the next `start()`.

### Static members

| Member                       | Description                                                            |
| ---------------------------- | --------------------------------------------------------------------- |
| `Marshall.Status`            | Enum of statuses (see below).                                         |
| `Marshall.abortAll()`        | Abort every task this process still holds; returns the count aborted. |
| `Marshall.isRunning(name)`   | `true` if the named task is currently `Started`.                      |
| `Marshall.isTimedOut(name)`  | `true` if a running task has exceeded its timeout.                    |
| `Marshall.Terminate(id)`     | Mark a running task as terminated.                                    |

### Status values

`Started`, `Completed`, `Disabled`, `NotRunning`, `Terminated`, `AlreadyRunning`,
`TooSoon`, `Error`, `Aborted`.

## Graceful shutdown

To avoid locking out a restarted process for the full task timeout, abort any locks still
held when the process is shutting down:

```js
process.on('SIGTERM', async () => {
  await Marshall.abortAll();
  process.exit(0);
});
```

## Scripts

```sh
npm run check   # syntax-check the module (node --check)
```

## License

UNLICENSED — private.
