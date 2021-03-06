# PostgreSQL Waterline Adapter

[![NPM version](https://img.shields.io/npm/v/postgresql-waterline-adapter.svg?style=flat)](https://npmjs.org/package/postgresql-waterline-adapter)
[![Known Vulnerabilities](https://snyk.io/test/npm/postgresql-waterline-adapter/badge.svg)](https://snyk.io/test/npm/postgresql-waterline-adapter)

A bare-bones Waterline adapter for [PostgreSQL](http://www.postgresql.org/). It does not support schema creation/modification or streaming data.

## Compatibility
- Waterline v0.12.x
- PostgreSQL 9.6.x

## Install

```sh
$ npm install postgres-waterline-adapter
```

## Configuration

#### `config/connections.js`

```js
module.exports.connections = {
  // ...
  postgres: {
    connection: {
      database: 'databaseName',
      host: 'localhost',
      user: 'user',
      password: 'password',
      port: 5432,
      ssl: false
    },
  },
  // Or use a url string
  postgres: {
    connection: {
      url: 'postgresql://user:password@localhost:5432/databaseName?ssl=false',
    },
  }
}
```

## License
MIT
