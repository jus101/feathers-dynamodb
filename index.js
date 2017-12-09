const dynamo = require('dynamodb');
const Proto = require('uberproto');
const { pick, isArray, map } = require('lodash');

const DEFAULT_ID_FIELD = 'id';
const RESERVED_ATTRIBUTE_NAMES = require('./lib/reserved-attribute-names');

const mapExpressionAttributeNames = function(names) {
  return names.map(name => {
    const upper = name.toUpperCase();
    if (~RESERVED_ATTRIBUTE_NAMES.indexOf(upper)) {
      return [`#${name}`, name];
    } else {
      return name;
    }
  });
}

class Service {
  constructor (options) {
    if (!(options && options.Model)) {
      throw new Error('DynamoDB options and options.Model must be supplied');
    }
    this.id = options.id || DEFAULT_ID_FIELD;
    this.events = options.events || [];
    this.paginate = options.paginate || {};
    this.Model = options.Model;
  }

  parseParams(_params) {
    const params = {
      ExpressionAttributeNames: {}
    };

    if (_params.query && _params.query.$select && Array.isArray(_params.query.$select)) {
      const select = _params.query.$select;

      if (!~select.indexOf(this.id)) select.push(this.id);

      const mapped = mapExpressionAttributeNames(select);

      params.ProjectionExpression = mapped.map(attr => {
        if (Array.isArray(attr)) {
          params.ExpressionAttributeNames[attr[0]] = attr[1];
          return attr[0];
        }
        return attr;
      }).join(', ');
    }

    if (!Object.keys(params.ExpressionAttributeNames).length) {
      delete params.ExpressionAttributeNames;
    }

    console.log('PARAMS', params);

    return params;
  }

  postSelect(doc, params) {
    if (params.query && params.query.$select && Array.isArray(params.query.$select)) {
      const out = pick(doc.get(), params.query.$select);
      out[this.id] = doc.get(this.id);
      return out;
    }
    return doc.get();
  }

  find(params) {
    if (!params.query) {
      return this.scan(params);
    }
  }

  scan(params) {
    return new Promise((resolve, reject) => {
      this.Model
        .scan()
        .loadAll()
        .exec()
      });
  }

  get (id, params) {
    return new Promise((resolve, reject) => {
      this.Model.get(id, this.parseParams(params), (err, doc) => {
        if (err) return reject(err);
        return resolve(doc ? doc.get() : undefined);
      });
    });
  }

  create (data, params) {
    return new Promise((resolve, reject) => {
      this.Model.create(data, (err, doc) => {
        if (err) return reject(err);
        return resolve(doc ? (isArray(doc)? map(doc, (d) => d.get()) : doc.get() ): undefined);
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

  remove(id, _params) {
    if (id === null) {
      throw new Error('Bulk remove operation not supported');
    }
    const params = {};
    params.ReturnValues = 'ALL_OLD';

    return new Promise((resolve, reject) => {
      this.Model.destroy(id, params, (err, doc) => {
        if (err) return reject(err);
        return resolve(doc ? this.postSelect(doc, _params) : undefined);
      });
    });
  }

  extend (obj) {
    return Proto.extend(obj, this);
  }
}

function init (options) {
  return new Service(options);
}

module.exports = init;
init.Service = Service;
