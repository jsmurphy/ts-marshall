import os from 'node:os';

/**
 * Distributed task-lock primitive ("Marshall").
 *
 * This module is dependency-injected and intentionally free of any service-specific imports so it
 * can be shared verbatim across services. Each consumer keeps a tiny shim that injects its own
 * Mongo collections map and logger, then re-exports the bound class as the default export:
 *
 * ```js
 * // repo-local modules/.../marshall.mjs (shim)
 * import { coll } from './conn.mjs';
 * import { logger } from './logger.mjs';
 * import { createMarshall } from '@ts/marshall';
 *
 * export default createMarshall({ coll, logger });
 * ```
 *
 * Every existing call site (`import Marshall from './marshall.mjs'; new Marshall(id)`) keeps working
 * unchanged.
 *
 * @param {object} deps
 * @param {object} deps.coll        Map of Mongo collection handles, keyed by name (accessed lazily).
 * @param {object} [deps.logger]    Pino-style logger ({obj}, msg). Defaults to a no-op logger.
 * @param {number} [deps.hardTimeout=20]  Default task timeout in minutes (long enough for an initial sync).
 * @param {string} [deps.tasksName='tasks']  Name of the tasks collection within `coll`.
 * @returns {typeof Marshall}
 */
export function createMarshall({ coll, logger, hardTimeout = 20, tasksName = 'tasks' } = {}) {
  if (!coll) {
    throw new Error('createMarshall requires a `coll` collections map');
  }

  const NOOP = () => {};
  const log = logger ?? { info: NOOP, warn: NOOP, error: NOOP, debug: NOOP };

  // Resolve the tasks collection lazily so injection works even when the connection is established
  // after this module is evaluated.
  const tasks = () => coll[tasksName];

  // Live, started Marshall instances in this process. A graceful shutdown can release any locks
  // still held (e.g. by a task that exited before reaching a cooperative checkpoint) so the next
  // process can re-acquire them immediately instead of waiting out the task timeout.
  const activeMarshalls = new Set();

  const Marshall = class Marshall {
    /**
     * @param {string} id
     */
    constructor(id) {
      if (!id) {
        throw new Error('Must pass id to Marshall constructor');
      }

      // Configurable properties
      this.id = id;
      this.force = false;
      this.started = false;
      // Internals
      this._mins = 1;
      this._timeout = hardTimeout;
      this._errored = false;
      this._aborted = false;
    }

    /**
     * @param {number} minutes
     */
    mins(minutes) {
      if (!Number.isInteger(minutes) || minutes < 1) throw new Error('Invalid mins specified');
      this._mins = minutes;
    }

    /**
     * @param {number} minutes
     */
    timeout(minutes) {
      if (!Number.isInteger(minutes) || minutes < 1) {
        throw new Error('Invalid timeout specified');
      }
      this._timeout = minutes;
    }

    static Status = {
      Started: 'Started',
      Completed: 'Completed',
      Disabled: 'Disabled',
      NotRunning: 'NotRunning',
      Terminated: 'Terminated',
      AlreadyRunning: 'AlreadyRunning',
      TooSoon: 'TooSoon',
      Error: 'Error',
      Aborted: 'Aborted',
    };

    /**
     * Abort every task this process still holds a lock on. Called from the graceful-shutdown path so
     * a restarted process is not locked out for the full task timeout.
     * @returns {Promise<number>} the number of tasks aborted.
     */
    static async abortAll() {
      const active = [...activeMarshalls];
      await Promise.allSettled(active.map((m) => m.abort()));
      return active.length;
    }

    static async isRunning(name) {
      const result = await tasks().findOne({ _id: name, status: Marshall.Status.Started });
      return result !== null;
    }

    static async isTimedOut(name) {
      const result = await tasks().findOne({ _id: name, status: Marshall.Status.Started });
      if (!result?.timeout) return false;
      return new Date() > new Date(result.start.getTime() + result.timeout * 60 * 1000);
    }

    static async Terminate(id) {
      try {
        const result = await tasks().findOneAndUpdate(
          {
            _id: id,
            status: Marshall.Status.Started,
          },
          {
            $set: {
              status: Marshall.Status.Error,
              error: { name: Marshall.Status.Terminated },
              end: new Date(),
            },
            $unset: { result: '' },
          },
        );

        if (result) {
          log.info({ id, result }, 'Marked as terminated');
          return { ok: true, id, status: Marshall.Status.Terminated };
        }

        return { ok: true, id, status: Marshall.Status.NotRunning };
      } catch (err) {
        if (err.codeName !== 'DuplicateKey') {
          log.error({ err, id }, 'Error terminating task');
        }
        return { ok: false, id, status: Marshall.Status.Error, error: err.message };
      }
    }

    async start() {
      this.force |= await this.#isForceNext();

      try {
        if (this.force) {
          return this.#forceStart();
        }

        const record = await tasks().findOne({ _id: this.id }, { projection: { enabled: 1 } });
        if (record?.enabled === false) {
          log.debug({ id: this.id }, 'Task is administratively disabled');
          return { started: false, status: Marshall.Status.Disabled };
        }

        const result = await this.#attemptStart();

        if (result.status == Marshall.Status.TooSoon) {
          if (await this.#alreadyRunning()) {
            return { started: false, status: Marshall.Status.AlreadyRunning };
          }
          return { started: false, status: Marshall.Status.TooSoon };
        }

        if (result.status !== Marshall.Status.Started || result.host !== os.hostname()) {
          log.info({ result }, 'Concurrent synchronisation detected, not starting');
          return { started: false, status: Marshall.Status.AlreadyRunning, host: result.host };
        }
      } catch (ex) {
        // If the previous run is too soon we'll encounter a DuplicateKey
        if (ex.codeName !== 'DuplicateKey') {
          log.warn({ err: ex }, 'Error starting task');
          return { started: false, status: Marshall.Status.Error, error: ex.message };
        }

        if (await this.#alreadyRunning()) {
          return { started: false, status: Marshall.Status.AlreadyRunning };
        }

        return { started: false, status: Marshall.Status.TooSoon };
      }

      this.started = true;
      activeMarshalls.add(this);
      return { started: true };
    }

    async finish(metadata) {
      activeMarshalls.delete(this);
      // If we have already errored or aborted don't check if running
      if (this._errored || this._aborted) return;
      // If we were never started do nothing
      if (!this.started) return;

      // Update status in database
      try {
        const update = {
          $set: { status: Marshall.Status.Completed, host: os.hostname(), end: new Date() },
          $unset: { error: '' },
        };

        // Persist any extra metadata supplied
        if (metadata?.constructor === Object)
          Object.keys(metadata).forEach((key) => {
            if (metadata[key] === null) {
              update.$unset[key] = '';
            } else {
              update.$set[key] = metadata[key];
            }
          });

        await tasks().findOneAndUpdate({ _id: this.id, status: Marshall.Status.Started }, update);
      } catch (err) {
        if (err.codeName !== 'DuplicateKey') {
          log.warn({ err }, 'Error updating task');
        }
      }
    }

    async error(error) {
      this._errored = true;
      activeMarshalls.delete(this);

      try {
        await tasks().findOneAndUpdate(
          {
            _id: this.id,
            status: Marshall.Status.Started,
          },
          {
            $set: {
              status: Marshall.Status.Error,
              error: {
                name: error?.name,
                message: error?.message || error,
                stack: error?.stack,
              },
              end: new Date(),
            },
            $unset: { result: '' },
          },
        );
      } catch (err) {
        if (err.codeName !== 'DuplicateKey') {
          log.error({ err }, 'Error updating task status');
        }
      }
    }

    /**
     * Release the lock for a cooperative shutdown without recording a failure.
     *
     * Marks the task `Aborted` and backdates its start so a freshly-restarted process can re-acquire
     * it immediately (rather than waiting out the task timeout), while keeping it distinct from a
     * real `Error` so monitoring is not polluted with shutdown noise.
     */
    async abort() {
      if (this._errored || this._aborted) return;
      this._aborted = true;
      activeMarshalls.delete(this);
      if (!this.started) return;

      try {
        await tasks().findOneAndUpdate(
          {
            _id: this.id,
            status: Marshall.Status.Started,
          },
          {
            $set: { status: Marshall.Status.Aborted, host: os.hostname(), end: new Date(), start: new Date(0) },
            $unset: { error: '', result: '' },
          },
        );
      } catch (err) {
        if (err.codeName !== 'DuplicateKey') {
          log.warn({ err, id: this.id }, 'Error aborting task');
        }
      }
    }

    /**
     * Sets the task as terminated in the database.
     * Does not perform any termination.
     */
    async terminate() {
      return Marshall.Terminate(this.id);
    }

    async #alreadyRunning() {
      const running = await tasks().findOne({ _id: this.id, status: Marshall.Status.Started });
      return !!running;
    }

    async #forceStart() {
      const _start = new Date().setSeconds(0, 0);

      await tasks().findOneAndUpdate(
        {
          _id: this.id,
        },
        {
          $set: { status: Marshall.Status.Started, start: new Date(_start), host: os.hostname(), timeout: this._timeout },
          $unset: { error: '', result: '' },
        },
        {
          upsert: true,
        },
      );

      this.started = true;
      activeMarshalls.add(this);

      return { started: true };
    }

    async #attemptStart() {
      const _start = new Date().setSeconds(0, 0);
      const timeout = new Date(_start - this._timeout * 60 * 1000);
      const last_allowed_start = new Date(_start - this._mins * 60 * 1000);

      const timed_out = await tasks().findOneAndUpdate(
        {
          _id: this.id,
          status: Marshall.Status.Started,
          start: { $lt: timeout },
        },
        {
          $set: { status: Marshall.Status.Started, start: new Date(_start), host: os.hostname(), timeout: this._timeout },
          $unset: { error: '', result: '' },
        },
        {
          returnDocument: 'after',
        },
      );

      if (timed_out) {
        log.warn({ id: this.id, timeout: this._timeout }, 'Last task run timed out - restarting task');
        return timed_out;
      }

      return tasks().findOneAndUpdate(
        {
          _id: this.id,
          status: { $in: [Marshall.Status.Completed, Marshall.Status.Error, Marshall.Status.Aborted] },
          start: { $lte: last_allowed_start },
        },
        {
          $set: { status: Marshall.Status.Started, start: new Date(_start), host: os.hostname(), timeout: this._timeout },
          $unset: { error: '', result: '' },
        },
        {
          upsert: true,
          returnDocument: 'after',
        },
      );
    }

    async #isForceNext() {
      const result = await tasks().findOneAndUpdate(
        {
          _id: this.id,
          status: { $in: [Marshall.Status.Completed, Marshall.Status.Error] },
          force: true,
        },
        {
          $unset: { force: '' },
        },
      );

      if (result === null) return false;

      if (result) {
        log.info({ host: os.hostname() }, 'Force next sync flag enabled');
        return true;
      }

      return true;
    }
  };

  return Marshall;
}

export default createMarshall;
