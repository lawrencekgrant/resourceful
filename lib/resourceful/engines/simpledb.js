var resourceful = require('../../resourceful');
var Cache = resourceful.Cache;
var uuid = require('node-uuid');
var util = require('utile');
var winston = require('winston');

exports.stores = {};
exports.caches = {};

var SimpleDB = exports.Simpledb = function(options) {
  options = options || {};
  this.accessKey = options.accessKey;
  this.secretKey = options.secretKey;
  this.domainPrefix = options.domainPrefix;
  this.resource = options.resource; //I need to find a way to pull this from the object
  if (!this.domainPrefix) {
    throw new Error('A domain prefix has not been specified. The domain' +
    ' prefix is a required field.');
  }

  winston.info(util.inspect(this));

  //create simpledb object
  this.sdb = new require('simpledb').SimpleDB({
    keyid: this.accessKey,
    secret: this.secretKey
  });
  this.sdb.createDomain(
    this.domainPrefix + this.resource, function(err, res, meta) {
    if (err)
    {
      winston.error(err)
      throw err;
    } else {
      winston.info(JSON.stringify(res));
    }
  });

  this.cache = new resourceful.Cache();
}

SimpleDB.prototype.protocol = 'simple';

//not quite sure what this is for
SimpleDB.prototype.load = function(data) {
  winston.error('Load is not required.' + util.inspect(data));
  throw new (Error)('Load not valid for simpledb engine.');
}

SimpleDB.prototype.request = function(method) {
  //todo: implement if I need to...
  winston.debug(util.inspect(method));
};

SimpleDB.prototype.exists = function(item, callback) {

};

SimpleDB.prototype.find = function(properties, callback) {
  var resource = this.domainPrefix + properties.resource.toLowerCase();

  var whereClause = ' where '
  var counter = 0;

  var whereParts = new Array();

  for (var propName in properties) {
    counter++
    if (propName != 'resource') {
      whereParts.push(propName + ' = ' + '\'' + properties[propName] + '\'');
    }
  }
  whereClause += whereParts.join(' and ');
  if (whereParts.length < 1) whereClause = '';
  var selectString = 'select * from ' + resource + whereClause;

  this.sdb.select(selectString, function(err, res, meta) {
    if (err)
    {
      winston.info(JSON.stringify(err));
      callback(err)
    }
    else if (res.length > 0)
    {
      res.forEach(function(itm, id) {
        winston.info(id + '::' + itm);
        if (itm.$ItemName)
          delete itm.$ItemName;
      });
    } else {
      delete res.$ItemName;
    }
    callback(err, res);
  });
};

SimpleDB.prototype.destroy = function(item, callback) {
  winston.info('killing ' + item);
  var resource = (item.split('/')[0]).toLowerCase();
  var id = item.split('/')[1];
  var domainName = this.domainPrefix + resource.toLowerCase();

  var _this = this;
  this.sdb.deleteItem(domainName, id, function(err, res) {
    if (err)
      winston.info(JSON.stringify(err));
    callback(err, res);
  });
}

SimpleDB.prototype.get = function(item, callback) {
  var resource = (item.split('/')[0]).toLowerCase();
  var id = item.split('/')[1];
  var domainName = this.domainPrefix + resource.toLowerCase();
  this.sdb.getItem(domainName, id, function(err, res, meta) {
    if (err) {
      callback(err);
    } else {
      if (!res) {
        callback(
          {status:404, message: 'resource not found'},
          {is:'broken'}
        );
        return;
      } else if (res instanceof Array) {
        res.forEach(function(itm) {
          if (itm.$ItemName) {
            delete itm.$ItemName;
          }
        });
        callback(null, res);
      } else {
        delete res.$ItemName;
      }
      callback(err, res);
    }
  });
}

SimpleDB.prototype.save = function(id, data, callback) {
  var nameid = id.split('/');
  data.id = nameid[1];
  var domainName = this.domainPrefix + data.resource.toLowerCase();
  winston.debug('Saving to ' + domainName + '...' + util.inspect(data));
  this.sdb.putItem(domainName, data.id, data, function(err, res, meta) {
    if (err) {
      winston.error(util.inspect(err));
      callback(err, res);
    } else {
      winston.debug('Response: ' + JSON.stringify(res));
      callback(err, data);
    }
  });
};

SimpleDB.prototype.update = function(id, data, callback) {
  this.save(id, data, callback);
}
