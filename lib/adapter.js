'use strict';

const _ = require('lodash');
const {
  Pool,
} = require('pg');
const WaterlineSequel = require('waterline-sequel');
const WaterlineErrors = require('waterline-errors').adapter;

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
    const connection = this.connections[connectionName];
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

    for (const [modelName, model] of models.entries()) {
      let waterlineSchema;
      if (model.waterline && model.waterline.schema) {
        waterlineSchema = model.waterline.schema[connection.identity];
      }

      if (waterlineSchema && waterlineSchema.connection.connection.includes(connection.identity)) {
        const dataCastMethods = this._getDataCastMethods(model.definition || {});
        const dataCastAttributeNames = _.keys(dataCastMethods);

        schema[modelName] = {
          meta: model.meta || {},
          tableName: waterlineSchema.tableName || modelName,
          connection: waterlineSchema.connection,
          definition: model.definition || {},
          dataCastMethods,
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
      const connection = this.connections[connectionName];
      if (!connection) {
        throw WaterlineErrors.InvalidConnection;
      }

      const {
        schema,
      } = connection;
      const waterlineSequel = new WaterlineSequel(schema);
      // If this is using an older version of the Waterline API and a select
      // modifier was used, translate to use columnName values.
      if (schema.version < 1 && queryObject.select) {
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

      const query = waterlineSequel.find(modelName, queryObject);
      const db = this._getDatabaseConnection(connectionName);

      return db.query(query.query, query.values).then((result) => {
        if (schema[modelName] && _.some(schema[modelName].dataCastAttributeNames)) {
          const results = [];
          for (const row of result.rows) {
            const clonedRow = _.cloneDeep(row);
            for (const attributeName of schema[modelName].dataCastAttributeNames) {
              clonedRow[attributeName] = schema[modelName].dataCastMethods[attributeName](attributeName);
            }

            results.push(clonedRow);
          }

          return results;
        }

        if (_.isFunction(cb)) {
          cb(null, result.rows);
        }

        return result.rows;
      });
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
      const connection = this.connections[connectionName];
      if (!connection) {
        throw WaterlineErrors.InvalidConnection;
      }

      const {
        schema,
      } = connection;
      const waterlineSequel = new WaterlineSequel(schema);
      const query = waterlineSequel.count(modelName, queryObject);
      const db = this._getDatabaseConnection(connectionName);

      return db.query(query.query, query.values).then((result) => {
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
      });
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
      const connection = this.connections[connectionName];
      if (!connection) {
        throw WaterlineErrors.InvalidConnection;
      }

      const {
        schema,
      } = connection;
      const waterlineSequel = new WaterlineSequel(schema);
      const query = waterlineSequel.create(modelName, data);
      const db = this._getDatabaseConnection(connectionName);

      return db.query(query.query, query.values).then((result) => {
        if (schema[modelName] && _.some(schema[modelName].dataCastAttributeNames)) {
          const results = [];
          for (const row of result.rows) {
            const clonedRow = _.cloneDeep(row);
            for (const attributeName of schema[modelName].dataCastAttributeNames) {
              clonedRow[attributeName] = schema[modelName].dataCastMethods[attributeName](attributeName);
            }

            results.push(clonedRow);
          }

          return results;
        }

        if (_.isFunction(cb)) {
          cb(null, result.rows);
        }

        return result.rows;
      });
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

      const connection = this.connections[connectionName];
      if (!connection) {
        throw WaterlineErrors.InvalidConnection;
      }

      const {
        schema,
      } = connection;
      const waterlineSequel = new WaterlineSequel(schema);
      const query = waterlineSequel.update(modelName, queryObject, data);
      const db = this._getDatabaseConnection(connectionName);

      return db.query(query.query, query.values).then((result) => {
        if (schema[modelName] && _.some(schema[modelName].dataCastAttributeNames)) {
          const results = [];
          for (const row of result.rows) {
            const clonedRow = _.cloneDeep(row);
            for (const attributeName of schema[modelName].dataCastAttributeNames) {
              clonedRow[attributeName] = schema[modelName].dataCastMethods[attributeName](attributeName);
            }

            results.push(clonedRow);
          }

          return results;
        }

        if (_.isFunction(cb)) {
          cb(null, result.rows);
        }

        return result.rows;
      });
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
      const connection = this.connections[connectionName];
      if (!connection) {
        throw WaterlineErrors.InvalidConnection;
      }

      const {
        schema,
      } = connection;
      const waterlineSequel = new WaterlineSequel(schema);
      const query = waterlineSequel.destroy(modelName, queryObject);
      const db = this._getDatabaseConnection(connectionName);

      return db.query(query.query, query.values).then((result) => {
        if (schema[modelName] && _.some(schema[modelName].dataCastAttributeNames)) {
          const results = [];
          for (const row of result.rows) {
            const clonedRow = _.cloneDeep(row);
            for (const attributeName of schema[modelName].dataCastAttributeNames) {
              clonedRow[attributeName] = schema[modelName].dataCastMethods[attributeName](attributeName);
            }

            results.push(clonedRow);
          }

          return results;
        }

        if (_.isFunction(cb)) {
          cb(null, result.rows);
        }

        return result.rows;
      });
    } catch (ex) {
      if (cb) {
        return cb(ex);
      }

      throw ex;
    }
  },

  // NOTE: TODO: Maybe someday an optimized join can be available...
  //join(connectionName, modelName, queryObject, cb) {
  // try {
  //   const connection = this.connections[connectionName];
  //   if (!connection) {
  //     throw WaterlineErrors.InvalidConnection;
  //   }
  //
  //   const {schema} = connection;
  //   const modelSchema = schema[modelName];
  //   let primaryKeyColumnName = 'id';
  //
  //   for (const [key, value] of modelSchema.definition.entries()) {
  //     if (value.primaryKey) {
  //       primaryKeyColumnName = value.columnName || key;
  //       break;
  //     }
  //   }
  //
  //
  // } catch (ex) {
  //   if (cb) {
  //     return cb(ex);
  //   }
  //
  //   throw ex;
  // }
  //},

  stream() {
    // Omitting this as we do not use waterline to stream records
    throw new Error('stream() is not supported with postgres-waterline-adapter.');
  },

  /**
   * Gets a dictionary of methods to use for attributes in a schema that require data casting from postgres, with key equal to the attribute name
   * @param {Object} definition - Model schema definition
   * @returns {Object} - Key is the name of the schema attribute
   * @private
   */
  _getDataCastMethods(definition) {
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
};
