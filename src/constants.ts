import { IDriverAlias } from '@sqltools/types';

const { displayName } = require('../package.json');

/**
 * Aliases for your driver. EG: PostgreSQL, PG, postgres can all resolve to your driver
 */
export const DRIVER_ALIASES: IDriverAlias[] = [{ displayName: displayName, value: displayName }];
