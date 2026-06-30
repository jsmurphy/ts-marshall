/**
 * Example repo-local Marshall shim.
 *
 * Drop a file like this into each consuming service (replacing the current copy of marshall.mjs),
 * adjusting only the two dependency imports to that repo's paths:
 *
 *   wfm-service : import { coll } from './conn.mjs';      import { logger } from './logger.mjs';
 *   ts-slurm    : import { coll } from './conn.mjs';      import logger from './logger.mjs';
 *   slackTC     : import { coll } from './conn.mjs';      import { logger } from './logger.mjs';
 *   dsat        : import { coll } from '../modules/db.mjs';  // no logger -> omit it
 *
 * Every existing `import Marshall from './marshall.mjs'; new Marshall(id)` keeps working unchanged.
 */
import { coll } from './conn.mjs';
import { logger } from './logger.mjs';
import { createMarshall } from '@ts/marshall';

export default createMarshall({ coll, logger });
