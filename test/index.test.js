const { expect } = require('chai');
const { base } = require('feathers-service-tests');
const feathers = require('feathers');
const errors = require('feathers-errors');
const service = require('../');
const joi = require('joi');
const dynamodb = require('dynamodb');
const { promisify } = require('util');

dynamodb.AWS.config.update({
  region: 'local',
  endpoint : 'http://localhost:8000',
  apiVersion: '2012-08-10'
});

describe('Feathers DynamoDB Service', () => {
  const app = feathers();
  const createTables = promisify(dynamodb.createTables);

  const Model = dynamodb.define('People', {
    hashKey: 'id',
    schema: {
      id: dynamodb.types.uuid(),
      name: joi.string(),
      age: joi.number(),
      time: joi.number(),
      created: joi.boolean()
    }
  });

  before(async () => {
    await createTables({
      'people': { readCapacity: 1, writeCapacity: 1 }
    });

    app.use('/people', service({ Model, events: [ 'testing' ] }));
  });

  base(app, errors, 'people', 'id');

  after(() => {
    return new Promise((resolve, reject) => {
      Model.deleteTable(err => err ? reject(err) : resolve());
    });
  });
});
