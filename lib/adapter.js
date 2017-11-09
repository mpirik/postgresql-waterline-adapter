'use strict';

const _ = require('lodash');
const {
  Pool,
} = require('pg');
const WaterlineSequel = require('waterline-sequel');
const WaterlineErrors = require('waterline-errors').adapter;
const CriteriaProcessor = require('waterline-sequel/sequel/lib/criteriaProcessor');
const WaterlineSequelUtils = require('waterline-sequel/sequel/lib/utils');

/**
 * Redefined query builder to convert/cast queries parameters (eg. cast JSON type field to string when value is a string)
 * @param {string} tableName
 * @param {string} parent - Name of the column being queried
 * @param {*} value
 * @param {string} combinator
 * @param {Boolean} sensitive
 */
CriteriaProcessor.prototype.processSimple = function processSimple(tableName, parent, value, combinator, sensitive) {
  const currentSchema = this.schema[tableName].definition;
  const parentSchema = currentSchema[parent];
  const sensitiveTypes = ['text', 'string'];
  let lower;
  let parentType;
  if (parentSchema) {
    parentType = parentSchema.type || parentSchema;

    lower = parentType && sensitiveTypes.indexOf(parentType) > -1;
  }

  // Check if value is a string and if so add LOWER logic
  // to work with case in-sensitive queries

  if (!sensitive && lower && _.isString(value)) {
    // Add LOWER to parent
    parent = this.buildParam(this.getTableAlias(), parent, true);
    value = value.toLowerCase();

  } else {
    // Escape parent
    parent = this.buildParam(this.getTableAlias(), parent, false);
  }

  if (value === null) {
    this.queryString += `${parent} IS NULL`;
    return this.queryString;
  }

  // Simple Key/Value attributes
  if (this.parameterized) {
    if (parentType === 'json' && _.isString(value)) {
      this.queryString += `${parent} ${combinator} to_jsonb($${this.paramCount}::text)`;
    } else {
      this.queryString += `${parent} ${combinator} $${this.paramCount}`;
    }
    this.values.push(value);
    this.paramCount = this.paramCount + 1;

    return;
  }

  // Check if the value is a DATE and if it's not a date turn it into one
  if (parentType === 'date' && !_.isDate(value)) {
    value = new Date(value);
  }

  if (_.isDate(value)) {
    const date = `${value.getFullYear()}-${
      (`00${value.getMonth() + 1}`).slice(-2)}-${
      (`00${value.getDate()}`).slice(-2)} ${
      (`00${value.getHours()}`).slice(-2)}:${
      (`00${value.getMinutes()}`).slice(-2)}:${
      (`00${value.getSeconds()}`).slice(-2)}`;

    value = date;
  }

  if (_.isString(value)) {
    value = `"${WaterlineSequelUtils.escapeString(value)}"`;
  }

  this.queryString += `${parent} ${combinator} ${value}`;
};

module.exports = {
  identity: 'postgres-waterline-adapter',
  syncable: true,
  defaults: {
    host: 'localhost',
    port: 5432,
    schema: true,
    ssl: false,
  },

  /**
   * Options to pass to waterline-sequel
   */
  _waterlineSequelOptions: {
    parameterized: true,
    caseSensitive: true,
    escapeCharacter: '"',
    casting: true,
    canReturnValues: true,
    escapeInserts: true,
    declareDeleteAlias: false,
    schemaName: {},
  },

  /**
   * Collection of database pools by connection name. Lazily loaded
   */
  _pools: {},

  /**
   * Gets a database connection/pool for the specified connection name
   * @param {string} connectionName
   * @returns {Object}
   * @private
   */
  _getDatabaseConnection(connectionName) {
    const connection = this._connections[connectionName];
    if (!connection) {
      throw WaterlineErrors.InvalidConnection;
    }

    if (!this._pools[connectionName]) {
      this._pools[connectionName] = new Pool(connection.config);
    }

    return this._pools[connectionName];
  },

  /**
   * Database connection information
   * {
   *   connection-identity: {
   *     config: Object
   *     schema: Object
   *     version: Number
   *   }]
   * @private
   */
  _connections: {},

  /**
   * Register a new DB connection
   * @param {Object} connection - Connection info
   * @param {Object[]} models - Model schemas
   * @param {function} cb
   */
  registerConnection(connection, models, cb) {
    if (!connection.identity) {
      return cb(WaterlineErrors.IdentityMissing);
    }

    if (this._connections[connection.identity]) {
      return cb(WaterlineErrors.IdentityDuplicate);
    }

    const version = connection.version || 0;
    const schema = {};

    for (const [tableName, model] of Object.entries(models)) {
      let waterlineSchema;
      if (model.waterline && model.waterline.schema) {
        waterlineSchema = model.waterline.schema[tableName] || _.find(model.waterline.schema, {
          tableName,
        });
      }

      if (waterlineSchema && waterlineSchema.connection.includes(connection.identity)) {
        const definition = model.definition || {};
        const dataSendCastMethods = this._getDataSendCastMethods(definition);
        const dataReceiveCastMethods = this._getDataReceiveCastMethods(definition);
        const dataCastAttributeNames = _.keys(dataReceiveCastMethods);

        schema[tableName] = {
          meta: model.meta || {},
          tableName: waterlineSchema.tableName || tableName,
          connection: waterlineSchema.connection,
          definition: model.definition || {},
          dataSendCastMethods,
          dataReceiveCastMethods,
          dataCastAttributeNames,
          attributes: waterlineSchema.attributes || {},
        };
      }
    }

    this._connections[connection.identity] = {
      config: connection,
      schema,
      version,
    };

    if (_.isFunction(cb)) {
      cb();
    }
  },

  /**
   * Cleans up the adapter connections
   * @param {string} connectionName
   * @param {function} cb
   */
  teardown(connectionName, cb) {
    if (_.isFunction(connectionName)) {
      cb = connectionName;
      connectionName = null;
    }

    if (!connectionName) {
      this._connections = {};
    } else if (this._connections[connectionName]) {
      delete this._connections[connectionName];
    }

    if (_.isFunction(cb)) {
      cb();
    }
  },

  /**
   * Query data
   * @param {string} connectionName
   * @param {string} modelName
   * @param {string} query
   * @param {Object} data - Query parameters
   * @param {function} cb
   * @returns {Promise<Object>|Object}
   */
  query(connectionName, modelName, query, data, cb) {
    if (_.isFunction(data)) {
      cb = data;
      data = null;
    }

    try {
      const db = this._getDatabaseConnection(connectionName);

      if (data) {
        return db.query(query, data, cb);
      }

      return db.query(query, cb);
    } catch (ex) {
      if (cb) {
        return cb(ex);
      }

      throw ex;
    }
  },

  // region DB Schema CRUD
  describe(connectionName, table, cb) {
    // Omitting this as we do not use waterline to create/modify db schema
    if (_.isFunction(cb)) {
      cb();
    }
  },

  define(connectionName, table, definition, cb) {
    // Omitting this as we do not use waterline to create/modify db schema
    if (_.isFunction(cb)) {
      cb();
    }
  },

  createSchema(connectionName, table, schemaName, cb) {
    if (_.isFunction(schemaName)) {
      cb = schemaName;
      schemaName = null;
    }

    // Omitting this as we do not use waterline to create/modify db schema
    if (_.isFunction(cb)) {
      cb();
    }
  },

  drop(connectionName, table, relations, cb) {
    if (_.isFunction(relations)) {
      cb = relations;
      relations = [];
    }

    // Omitting this as we do not use waterline to create/modify db schema
    if (_.isFunction(cb)) {
      cb();
    }
  },

  addAttribute(connectionName, table, attributeName, attributeDefinition, cb) {
    // Omitting this as we do not use waterline to create/modify db schema
    if (_.isFunction(cb)) {
      cb();
    }
  },

  removeAttribute(connectionName, table, attributeName, cb) {
    // Omitting this as we do not use waterline to create/modify db schema
    if (_.isFunction(cb)) {
      cb();
    }
  },
  // endregion

  /**
   * Gets records
   * @param {string} connectionName
   * @param {string} modelName
   * @param {Object} queryObject - Object representing the query
   * @param {function} [cb]
   * @returns {Promise<Number>|Number}
   */
  find(connectionName, modelName, queryObject, cb) {
    try {
      const connection = this._connections[connectionName];
      if (!connection) {
        throw WaterlineErrors.InvalidConnection;
      }

      const {
        schema,
      } = connection;
      // If this is using an older version of the Waterline API and a select
      // modifier was used, translate to use columnName values.
      if (connection.version < 1 && queryObject.select) {
        const _select = [];
        for (const attributeName of queryObject.select) {
          if (connection.schema[modelName]) {
            const attributeDefinition = connection.schema[modelName].attributes[attributeName];
            if (attributeDefinition && attributeDefinition.columnName) {
              _select.push(attributeDefinition.columnName);
              // eslint-disable-next-line no-continue
              continue;
            }
          }

          _select.push(attributeName);
        }

        queryObject.select = _select;
      }

      const waterlineSequel = new WaterlineSequel(schema, this._waterlineSequelOptions);
      const query = waterlineSequel.find(modelName, queryObject);
      const db = this._getDatabaseConnection(connectionName);
      const shouldCast = schema[modelName] && _.some(schema[modelName].dataCastAttributeNames);

      return db.query(query.query[0], query.values[0]).then((result) => {
        if (shouldCast) {
          const results = [];
          for (const row of result.rows) {
            const clonedRow = _.cloneDeep(row);
            for (const attributeName of schema[modelName].dataCastAttributeNames) {
              clonedRow[attributeName] = schema[modelName].dataReceiveCastMethods[attributeName](result[attributeName]);
            }

            results.push(clonedRow);
          }

          if (_.isFunction(cb)) {
            cb(null, results);
          }

          return results;
        }

        if (_.isFunction(cb)) {
          cb(null, result.rows);
        }

        return result.rows;
      }).catch(this._handlePostgresError(cb, new Error().stack));
    } catch (ex) {
      if (cb) {
        return cb(ex);
      }

      throw ex;
    }
  },

  /**
   * Gets a count of records
   * @param {string} connectionName
   * @param {string} modelName
   * @param {Object} queryObject - Object representing the query
   * @param {function} [cb]
   * @returns {Promise<Number>|Number}
   */
  count(connectionName, modelName, queryObject, cb) {
    try {
      const connection = this._connections[connectionName];
      if (!connection) {
        throw WaterlineErrors.InvalidConnection;
      }

      const {
        schema,
      } = connection;
      const waterlineSequel = new WaterlineSequel(schema, this._waterlineSequelOptions);
      const query = waterlineSequel.count(modelName, queryObject);
      const db = this._getDatabaseConnection(connectionName);

      return db.query(query.query[0], query.values[0]).then((result) => {
        if (_.isFunction(cb)) {
          if (!_.isArray(result.rows) || !_.some(result.rows)) {
            return cb(new Error('Invalid query, no results returned.'));
          }

          cb(null, Number(result.rows[0].count));
        }

        if (!_.isArray(result.rows) || !_.some(result.rows)) {
          throw new Error('Invalid query, no results returned.');
        }

        return result.rows[0].count;
      }).catch(this._handlePostgresError(cb, new Error().stack));
    } catch (ex) {
      if (cb) {
        return cb(ex);
      }

      throw ex;
    }
  },

  /**
   * Creates a new record
   * @param {string} connectionName
   * @param {string} modelName
   * @param {Object} data - Values to insert
   * @param {function} [cb]
   * @returns {Promise<Object>|Object}
   */
  create(connectionName, modelName, data, cb) {
    try {
      const connection = this._connections[connectionName];
      if (!connection) {
        throw WaterlineErrors.InvalidConnection;
      }

      const {
        schema,
      } = connection;
      const waterlineSequel = new WaterlineSequel(schema, this._waterlineSequelOptions);
      const query = waterlineSequel.create(modelName, data);
      const db = this._getDatabaseConnection(connectionName);
      const shouldCast = schema[modelName] && _.some(schema[modelName].dataCastAttributeNames);
      const hasValues = query.query.indexOf('$1') > -1;
      if (shouldCast) {
        const dataKeys = _.keys(data);
        // Override values before sending to postgres
        for (const attributeName of schema[modelName].dataCastAttributeNames) {
          if (!_.isUndefined(data[attributeName])) {
            const dataIndex = _.indexOf(dataKeys, attributeName);
            if (dataIndex > -1) {
              query.values[dataIndex] = schema[modelName].dataSendCastMethods[attributeName](data[attributeName]);
            }
          }
        }
      }

      return db.query(query.query, hasValues ? query.values : undefined).then((result) => {
        if (shouldCast) {
          const row = _.cloneDeep(result.rows[0]);
          for (const attributeName of schema[modelName].dataCastAttributeNames) {
            row[attributeName] = schema[modelName].dataReceiveCastMethods[attributeName](result[attributeName]);
          }

          if (_.isFunction(cb)) {
            cb(null, row);
          }
          return row;
        }

        if (_.isFunction(cb)) {
          cb(null, result.rows[0]);
        }

        return result.rows[0];
      }).catch(this._handlePostgresError(cb, new Error().stack));
    } catch (ex) {
      if (cb) {
        return cb(ex);
      }

      throw ex;
    }
  },

  /**
   * Create multiple records
   * @param {string} connectionName
   * @param {string} modelName
   * @param {Object[]} records
   * @param {function} [cb]
   * @returns {Promise<Object[]>|Object[]}
   */
  createEach(connectionName, modelName, records, cb) {
    const results = [];
    const createStatements = records.map((data) => {
      return this.create(connectionName, modelName, data).then((result) => {
        results.push(result);
      });
    });

    return Promise.all(createStatements).then(() => {
      if (_.isFunction(cb)) {
        return cb(null, results);
      }

      return results;
    });
  },

  /**
   * Updates record(s)
   * @param {string} connectionName
   * @param {string} modelName
   * @param {Object} queryObject - Object representing the query
   * @param {Object} data - Values to update
   * @param {function} [cb]
   * @returns {Promise<Object[]>|Object[]}
   */
  update(connectionName, modelName, queryObject, data, cb) {
    try {
      // LIMIT in a postgresql UPDATE command is not valid
      if (_.has(queryObject, 'limit')) {
        throw new Error('LIMIT keyword is not allowed in the PostgreSQL UPDATE query.');
      }

      const connection = this._connections[connectionName];
      if (!connection) {
        throw WaterlineErrors.InvalidConnection;
      }

      const {
        schema,
      } = connection;
      const waterlineSequel = new WaterlineSequel(schema, this._waterlineSequelOptions);
      const query = waterlineSequel.update(modelName, queryObject, data);
      const db = this._getDatabaseConnection(connectionName);
      const shouldCast = schema[modelName] && _.some(schema[modelName].dataCastAttributeNames);
      const hasValues = query.query.indexOf('$1') > -1;
      if (shouldCast) {
        const dataKeys = _.keys(data);
        // Override values before sending to postgres
        for (const attributeName of schema[modelName].dataCastAttributeNames) {
          if (!_.isUndefined(data[attributeName])) {
            const dataIndex = _.indexOf(dataKeys, attributeName);
            if (dataIndex > -1) {
              query.values[dataIndex] = schema[modelName].dataSendCastMethods[attributeName](data[attributeName]);
            }
          }
        }
      }

      return db.query(query.query, hasValues ? query.values : undefined).then((result) => {
        if (shouldCast) {
          const results = [];
          for (const row of result.rows) {
            const clonedRow = _.cloneDeep(row);
            for (const attributeName of schema[modelName].dataCastAttributeNames) {
              clonedRow[attributeName] = schema[modelName].dataReceiveCastMethods[attributeName](result[attributeName]);
            }

            results.push(clonedRow);
          }

          if (_.isFunction(cb)) {
            cb(null, results);
          }

          return results;
        }

        if (_.isFunction(cb)) {
          cb(null, result.rows);
        }

        return result.rows;
      }).catch(this._handlePostgresError(cb, new Error().stack));
    } catch (ex) {
      if (cb) {
        return cb(ex);
      }

      throw ex;
    }
  },

  /**
   * Delete record(s)
   * @param {string} connectionName
   * @param {string} modelName
   * @param {Object} queryObject
   * @param {function} cb
   * @returns {Promise<Object[]>|Object[]}
   */
  destroy(connectionName, modelName, queryObject, cb) {
    try {
      const connection = this._connections[connectionName];
      if (!connection) {
        throw WaterlineErrors.InvalidConnection;
      }

      const {
        schema,
      } = connection;
      const waterlineSequel = new WaterlineSequel(schema, this._waterlineSequelOptions);
      const query = waterlineSequel.destroy(modelName, queryObject);
      const db = this._getDatabaseConnection(connectionName);
      const shouldCast = schema[modelName] && _.some(schema[modelName].dataCastAttributeNames);
      const hasValues = query.query.indexOf('$1') > -1;

      return db.query(query.query, hasValues ? query.values : undefined).then((result) => {
        if (shouldCast) {
          const results = [];
          for (const row of result.rows) {
            const clonedRow = _.cloneDeep(row);
            for (const attributeName of schema[modelName].dataCastAttributeNames) {
              clonedRow[attributeName] = schema[modelName].dataReceiveCastMethods[attributeName](result[attributeName]);
            }

            results.push(clonedRow);
          }

          if (_.isFunction(cb)) {
            cb(null, results);
          }

          return results;
        }

        if (_.isFunction(cb)) {
          cb(null, result.rows);
        }

        return result.rows;
      }).catch(this._handlePostgresError(cb, new Error().stack));
    } catch (ex) {
      if (cb) {
        return cb(ex);
      }

      throw ex;
    }
  },

  // NOTE: TODO: Maybe someday an optimized join can be available...
  //join(connectionName, modelName, queryObject, cb) {
  //},

  stream() {
    // Omitting this as we do not use waterline to stream records
    throw new Error('stream() is not supported with postgres-waterline-adapter.');
  },

  /**
   * Gets a dictionary of methods to use for attributes in a schema that require data casting before sending to postgres, with key equal to the attribute name
   * @param {Object} definition - Model schema definition
   * @returns {Object} - Key is the name of the schema attribute
   * @private
   */
  _getDataSendCastMethods(definition) {
    const methodsToCastByAttributeName = {};
    for (const attributeName of _.keys(definition)) {
      switch (definition[attributeName].type) {
        case 'array':
          methodsToCastByAttributeName[attributeName] = (value) => {
            return value;
          };
          break;
        default:
          break;
      }
    }

    return methodsToCastByAttributeName;
  },

  /**
   * Gets a dictionary of methods to use for attributes in a schema that require data casting from postgres, with key equal to the attribute name
   * @param {Object} definition - Model schema definition
   * @returns {Object} - Key is the name of the schema attribute
   * @private
   */
  _getDataReceiveCastMethods(definition) {
    const methodsToCastByAttributeName = {};
    for (const attributeName of _.keys(definition)) {
      switch (definition[attributeName].type) {
        case 'array':
          methodsToCastByAttributeName[attributeName] = (value) => {
            if (!value) {
              return value;
            }

            return JSON.parse(value);
          };
          break;
        default:
          break;
      }
    }

    return methodsToCastByAttributeName;
  },

  /**
   * Translate postgres error to waterline compatible error
   * @param {function} [cb]
   * @param {string} stack - Additional stack details to append to thrown error
   * @returns {Function}
   * @private
   */
  _handlePostgresError(cb, stack) {
    return function handlePostgresError(postgresError) {
      postgresError.stack = (postgresError.stack || '') + stack;
      if (_.isFunction(cb)) {
        return cb(postgresError);
      }

      return postgresError;
    };
  },
};
