const dynamo = require('dynamodb');

const DEFAULT_ID_FIELD = '_id';

class Service {
  constructor (options) {
    if (!options) {
      throw new Error('DynamoDB options have to be provided');
    }

    if (!options.schema) {
      throw new Error('DynamoDB options must define a schema');
    }

    if (!(options.aws && options.aws.region)) {
      throw new Error('DynamoDB requires \'aws.region\' option');
    }

    if (!options.table) {
      throw new Error('DynamoDB requires a \'table\' option');
    }

    dynamo.AWS.config.update(options.aws);

    this.id = options.id || DEFAULT_ID_FIELD;

    const _schema = {};
    _schema[this.id] = dynamo.types.uuid();

    const model = {
      hashKey: this.id,
      schema: Object.assign(_schema, options.schema)
    };

    if (options.rangeKey) {
      if (!model.schema.hasOwnProperty(options.rangeKey)) {
        throw new Error(`The rangeKey '${options.rangeKey}' is not defined in the model schema`);
      }

      model.rangeKey = options.rangeKey;
    }
    this.table = options.table;
    this.Model = dynamo.define(options.table, Object.assign(model, options.model || {}));
  }

  get (id, params) {
    return new Promise((resolve, reject) => {
      this.Model.get(id, params, (err, doc) => {
        if (err) return reject(err);
        return resolve(doc ? doc.get() : undefined);
      });
    });
  }

  create (data, params) {
    return new Promise((resolve, reject) => {
      this.Model.create(data, (err, doc) => {
        if (err) return reject(err);
        return resolve(doc ? doc.get() : undefined);
      });
    });
  }

  patch (id, data, params) {
    const patch = Object.assign({}, data);
    patch[this.id] = id;

    return new Promise((resolve, reject) => {
      this.Model.update(patch, (err, doc) => {
        if (err) return reject(err);
        return resolve(doc ? doc.get() : undefined);
      });
    });
  }

  update (id, data, params) {
    return this.patch(id, data, params);
  }

  remove(id, params) {
    return new Promise((resolve, reject) => {
      this.Model.destroy(id, params, (err, doc) => {
        if (err) return reject(err);
        return resolve(doc ? doc.get() : undefined);
      });
    });
  }

  createTable(readCapacity = 1, writeCapacity = 1) {
    return new Promise((resolve, reject) => {
      const tables = {};
      params[this.table] = { readCapacity, writeCapacity };
      dynamo.createTables(tables, err => err ? reject(err) : resolve())
    });
  }

  deleteTable() {
    return new Promise((resolve, reject) => {
      this.Model.deleteTable(err => err ? reject(err) : resolve());
    });
  }
}

function init (options) {
  return new Service(options);
}

module.exports = init;
init.Service = Service;
