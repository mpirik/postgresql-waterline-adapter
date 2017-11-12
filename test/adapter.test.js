'use strict';

const WaterlineSequel = require('waterline-sequel');
require('../lib/adapter');
require('chai').should();
const faker = require('faker');

describe('WaterlineSequel', () => {
  const schema = {
    foo_bar: {
      attributes: {
        id: {
          primaryKey: true,
          type: 'string',
          unique: true,
        },
        name: {
          required: true,
          type: 'string',
        },
        wibbleWobble: {
          columnName: 'wibble_wobble_id',
          foreignKey: true,
          on: 'id',
          onKey: 'id',
          references: 'wibblewobble',
          type: 'string',
        },
      },
      definition: {
        id: {
          primaryKey: true,
          type: 'string',
          unique: true,
        },
        name: {
          type: 'string',
        },
        wibble_wobble_id: {
          alias: 'wibbleWobble',
          foreignKey: true,
          model: 'wibblewobble',
          type: 'string',
        },
      },
      meta: {
        junctionTable: false,
      },
      tableName: 'foo_bar',
    },
    wibble_wobble: {
      attributes: {
        id: {
          primaryKey: true,
          type: 'string',
          unique: true,
        },
        name: {
          required: true,
          type: 'string',
        },
      },
      definition: {
        id: {
          primaryKey: true,
          type: 'string',
          unique: true,
        },
        name: {
          type: 'string',
        },
      },
      meta: {
        junctionTable: false,
      },
      tableName: 'wibble_wobble',
    },
  };
  const waterlineSchemaOptions = {
    parameterized: true,
    caseSensitive: true,
    escapeCharacter: '"',
    casting: true,
    canReturnValues: true,
    escapeInserts: true,
    declareDeleteAlias: false,
    schemaName: {},
  };

  describe('#processObject()', () => {
    it('should not lower string primary keys', () => {
      const waterlineSequel = new WaterlineSequel(schema, waterlineSchemaOptions);
      const id = faker.random.uuid();
      const query = waterlineSequel.find('foo_bar', {
        where: {
          id,
        },
      });

      query.query.should.have.length(1);
      query.query[0].should.equal('SELECT "foo_bar"."id", "foo_bar"."name", "foo_bar"."wibble_wobble_id" FROM "foo_bar" AS "foo_bar"  WHERE "foo_bar"."id" = $1 ');
      query.values[0].should.deep.equal([id]);
    });
    it('should handle hydrated query values', () => {
      const waterlineSequel = new WaterlineSequel(schema, waterlineSchemaOptions);
      const id = faker.random.uuid();
      const query = waterlineSequel.find('foo_bar', {
        where: {
          wibble_wobble_id: {
            id,
            name: faker.name.findName(),
          },
        },
      });

      query.query.should.have.length(1);
      query.query[0].should.equal('SELECT "foo_bar"."id", "foo_bar"."name", "foo_bar"."wibble_wobble_id" FROM "foo_bar" AS "foo_bar"  WHERE "foo_bar"."wibble_wobble_id" = $1  ');
      query.values[0].should.deep.equal([id]);
    });
    it('should handle query value [null, \'\']', () => {
      const waterlineSequel = new WaterlineSequel(schema, waterlineSchemaOptions);
      const query = waterlineSequel.find('foo_bar', {
        where: {
          wibble_wobble_id: {
            '!': [null, ''],
          },
        },
      });

      query.query.should.have.length(1);
      query.query[0].should.equal('SELECT "foo_bar"."id", "foo_bar"."name", "foo_bar"."wibble_wobble_id" FROM "foo_bar" AS "foo_bar"  WHERE "foo_bar"."wibble_wobble_id" IS NOT NULL AND "foo_bar"."wibble_wobble_id" NOT IN ($1)  ');
      query.values[0].should.deep.equal(['']);
    });
  });
});
