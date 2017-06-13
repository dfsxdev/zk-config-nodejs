const ZooKeeper = require('node-zookeeper-client');
const Promise = require('bluebird');

global.ConfigSetFlag = {
    CREATE: 1,
    CREATE_TEMPORARY: 2,
    OVERWRITE: 3,
    OVERWRITE_IF_EXISTS: 4,
    SEQUENTIAL: 5,
    SEQUENTIAL_TEMPORARY: 6
};

let cacheflag = {
    CACHEFLAG_INIT: 0,
    CACHEFLAG_DELETED: 1,
    CACHEFLAG_CREATED: 2,
    CACHEFLAG_CHANGEDD: 2
};

class ZKConfig {
    constructor() {
        let nodeEnv = process.env.NODE_ENV ? process.env.NODE_ENV : 'development';
        let connect = nodeEnv === 'development' ? '192.168.6.206:2181' : '192.168.6.206:2181';
        let timeout = 60000; // 单位毫秒

        this.defaultInitOpt = {
            connect,
            timeout
        };

        this.cachedata = {};
        this.childcachedata = {};
    }

    close() {
        let self = this;
        process.nextTick(function () {
            self.zookeeper.close();
        });
    }

    init(opt) {
        this.defaultInitOpt.connect = opt.connect;
        this.defaultInitOpt.timeout = opt.timeout;

        this.cachedata = {};
        this.childcachedata = {};

        this.bRestart = false;
        this.cacheSetHistory = {};

        this._initZook();
    }

    _initZook() {
        let self = this;

        this.zookeeper = ZooKeeper.createClient(this.defaultInitOpt.connect,
            {
                sessionTimeout: this.defaultInitOpt.timeout,
                spinDelay: 1000,
                retries: 0
            });

        this.zookeeper.once('connected', function () {
            self.log('Connected to the server.');

            if (self.bRestart) {
                self.bRestart = false;
                self.createCacheSetHistory();
            }
        });

        this.zookeeper.once('disconnected', function () {
            self.log('disconnected');
        });

        this.zookeeper.once('expired', function () {
            self.log('expired');

            self.bRestart = true;
            self._initZook();
        });

        this.zookeeper.connect();
    }

    createCacheSetHistory() {
        for (var path in this.cacheSetHistory) {
            if (this.cacheSetHistory.hasOwnProperty(path)) {
                this.set(path, this.cacheSetHistory[path].data, this.cacheSetHistory[path].flag);

            }
        }
    }

    log(str, ...args) {
        if (process.env.NODE_ENV != 'production') {
            if (args.length > 0) {
                console.log(str, args);
            } else {
                console.log(str);
            }

        }
    }


    setcache(path, value, flag = cacheflag.CACHEFLAG_CHANGEDD) {
        if (this.cachedata[path] == null)
            this.cachedata[path] = {};
        this.cachedata[path].flag = flag;
        this.cachedata[path].data = value;
    }

    getcache(path) {
        return this.cachedata[path];
    }

    setCacheSetHistory(path, value, flag = cacheflag.CACHEFLAG_CHANGEDD) {
        if (this.cacheSetHistory[path] == null)
            this.cacheSetHistory[path] = {};
        this.cacheSetHistory[path].flag = flag;
        this.cacheSetHistory[path].data = value;
    }

    getCacheSetHistory(path) {
        return this.cacheSetHistory[path];
    }


    setchildcache(path, value, flag = cacheflag.CACHEFLAG_CHANGEDD) {
        if (this.childcachedata[path] == null)
            this.childcachedata[path] = {};
        this.childcachedata[path].flag = flag;
        this.childcachedata[path].data = value;
    }

    getchildcache(path) {
        return this.childcachedata[path];
    }

    getzookflag(flag) {
        var zookflag = '';
        switch (flag) {
            case ConfigSetFlag.CREATE:
                zookflag = ZooKeeper.CreateMode.PERSISTENT;
                break;
            case ConfigSetFlag.CREATE_TEMPORARY:
                zookflag = ZooKeeper.CreateMode.EPHEMERAL;
                break;
            case ConfigSetFlag.OVERWRITE:
                zookflag = ZooKeeper.CreateMode.PERSISTENT;
                break;
            case ConfigSetFlag.OVERWRITE_IF_EXISTS:
                zookflag = ZooKeeper.CreateMode.PERSISTENT;
                break;
            case ConfigSetFlag.SEQUENTIAL:
                zookflag = ZooKeeper.CreateMode.PERSISTENT_SEQUENTIAL;
                break;
            case ConfigSetFlag.SEQUENTIAL_TEMPORARY:
                zookflag = ZooKeeper.CreateMode.EPHEMERAL_SEQUENTIAL;
                break;
            default:
                break;
        }

        return zookflag;
    }

    exists(path) {
        let self = this;
        return new Promise(function (resolve, reject) {
            self.zookeeper.exists(path, null, function (error, stat) {
                if (error) {
                    self.log('node exists, error: \'%s\'', error);
                    reject(error);
                }

                if (stat) {
                    self.log('Node exists.');
                    resolve(stat);
                    return;
                } else {
                    self.log('Node does not exist.');
                    reject(error);
                    return;
                }
            });
        });
    }

    get(path) {
        let self = this;
        return new Promise(function (resolve, reject) {
            let cache = self.getcache(path);
            if (cache != null) {
                // 因为watcher的是一次性的，所以节点被删除后，要再调用exists设置watcher。
                // 第一次get，标志为CACHEFLAG_DELETED，调用exists设置watcher。
                // 第二次get，标志为CACHEFLAG_INIT，就直接返回node not exists
                if (cache.flag === cacheflag.CACHEFLAG_DELETED) {
                    self.setcache(path, null, cacheflag.CACHEFLAG_INIT);
                } else if (cache.flag === cacheflag.CACHEFLAG_INIT) {
                    self.log('get cache node: node deleted');
                    reject('node not exists');
                    return;
                } else if (cache.data != null) {
                    let data = cache.data;
                    self.log('get cache node: ' + data);
                    resolve(data);
                    return;
                }
            }

            self.zookeeper.exists(path,
                function (event) {
                    self.log('exists, Got event: %s.', event);

                    // 节点被创建
                    if (event.getType() == ZooKeeper.Event.NODE_CREATED) {
                        self.log('node created');
                        self.setcache(path, null);
                    }
                },
                function (error, stat) {
                    if (error) {
                        self.log('node exists, error: \'%s\'', error);

                        reject(error);
                        return;
                    }

                    if (!stat) {
                        self.log('Node does not exist.');

                        // self.setcache(path, null, cacheflag.CACHEFLAG_DELETED);
                        reject('node not exists');

                        return;
                    } else {
                        self.zookeeper.getData(path,
                            function (event) {
                                self.log('getData, Got event: %s.', event);

                                if (event.getType() == ZooKeeper.Event.NODE_DELETED) {
                                    self.log('node deleted');
                                    self.setcache(path, null, cacheflag.CACHEFLAG_DELETED);
                                } else if (event.getType() == ZooKeeper.Event.NODE_DATA_CHANGED || event == ZooKeeper.Event.NODE_CREATED) {
                                    self.setcache(path, null);
                                }
                            },
                            function (error, data, stat) {
                                if (error) {
                                    self.log('zk node get error: "%s", stat=%s, data=%s', error, stat, data);
                                    reject(error);
                                    return;
                                } else {
                                    self.log('Got data: %s', data);
                                    self.setcache(path, data);
                                    resolve(data);
                                    return;
                                }
                            });
                    }
                });
        });
    }

    overwriteIfExists(inPath, value, flag, resolve, reject) {
        let self = this;

        var zookflag = self.getzookflag(flag);
        self.zookeeper.create(inPath, new Buffer(value), zookflag, function (error, path) {
            if (error) {
                let rc = error.getCode();
                if ((ZooKeeper.Exception.SYSTEM_ERROR < rc && rc < ZooKeeper.Exception.API_ERROR)
                    || rc == ZooKeeper.Exception.NO_AUTH
                    || rc == ZooKeeper.Exception.SESSION_EXPIRED
                    || rc == ZooKeeper.Exception.AUTH_FAILED
                    // || rc == ZooKeeper.Exception.ZCLOSING
                    || rc == ZooKeeper.Exception.NO_NODE) {
                    self.log('zk node create result: %d, error: "%s", path=%s', rc, error, path);

                    reject(error);
                    return;
                } else {
                    self.log('create failed, result: %d, error: "%s", path=%s', rc, error, path);
                    self.zookeeper.setData(inPath, new Buffer(value), -1, function (error, stat) {
                        if (error) {
                            let rc = error.getCode();
                            if ((ZooKeeper.Exception.SYSTEM_ERROR < rc && rc < ZooKeeper.Exception.API_ERROR)
                                || rc == ZooKeeper.Exception.NO_AUTH
                                || rc == ZooKeeper.Exception.SESSION_EXPIRED
                                || rc == ZooKeeper.Exception.AUTH_FAILED
                            // || rc == ZooKeeper.ZCLOSING
                            ) {
                                self.log('zk node set, other error, result: %d, error: "%s", stat=%s', rc, error, stat);

                                reject(error);
                                return;
                            } else {
                                self.log('zk node set, continue , result: %d, error: "%s", stat=%s', rc, error, stat);
                                self.overwriteIfExists(inPath, value, flag, resolve, reject);
                            }
                        } else {
                            self.log('set zk node succ!');

                            resolve(inPath);
                            return;
                        }
                    });
                }
            } else {
                self.log('create zk node succ! path=' + path);
                resolve(path);
                return;
            }
        });
    }

    set(path, value, flag) {
        if (flag == ConfigSetFlag.CREATE_TEMPORARY) {
            this.setCacheSetHistory(path, value, flag);
        }

        let zkData = null;
        let self = this;
        return new Promise(function (resolve, reject) {
            self.setcache(path, null);

            if (flag == ConfigSetFlag.OVERWRITE_IF_EXISTS) {
                self.overwriteIfExists(path, value, flag, resolve, reject);
            } else {
                if (flag == ConfigSetFlag.CREATE || flag == ConfigSetFlag.CREATE_TEMPORARY
                    || flag == ConfigSetFlag.SEQUENTIAL || flag == ConfigSetFlag.SEQUENTIAL_TEMPORARY) {
                    var zookflag = self.getzookflag(flag);
                    self.zookeeper.create(path, new Buffer(value), zookflag, function (error, path) {
                        if (error) {
                            self.log('zk node create error: "%s", path=%s', error, path);

                            reject(error);
                            return;
                        } else {
                            self.log('create zk node succ! path=' + path);

                            resolve(path);
                            return;
                        }
                    });
                } else if (flag == ConfigSetFlag.OVERWRITE) {
                    self.zookeeper.setData(path, new Buffer(value), -1, function (error, stat) {
                        if (error) {
                            self.log('zk node set error: "%s", stat=%s', error, stat);

                            reject(error);
                            return;
                        } else {
                            self.log('set zk node succ!');

                            resolve(path);
                            return;
                        }
                    });
                }
            }
        });
    }

    getChildPaths(path) {
        let self = this;
        return new Promise(function (resolve, reject) {
            let cache = self.getchildcache(path);
            if (cache != null) {
                if (cache.flag === cacheflag.CACHEFLAG_DELETED) {
                    self.setchildcache(path, null, cacheflag.CACHEFLAG_INIT);
                } else if (cache.flag === cacheflag.CACHEFLAG_INIT) {
                    self.log('get cache node: node deleted');
                    resolve(null);
                    return;
                } else if (cache.data != null) {
                    let data = cache.data;
                    self.log('get cache node: ' + data);
                    resolve(data);
                    return;
                }
            }

            self.zookeeper.exists(path, null, function (error, stat) {
                if (error) {
                    var rc = error.getCode();
                    self.log('node not exists, result: %d, error: \'%s\', stat=%j', rc, error, stat);
                    if (rc == ZooKeeper.Exception.NO_NODE) {
                        // self.setchildcache(path, null, cacheflag.CACHEFLAG_DELETED);
                        reject('node not exists');
                    } else {
                        reject(error);
                    }
                    return;

                } else {
                    self.zookeeper.getChildren(path,
                        function (event) {
                            self.log('getChildren, Got event: %s.', event);
                            if (event.getType() == ZooKeeper.Event.NODE_DELETED) {
                                self.setchildcache(path, null, cacheflag.CACHEFLAG_DELETED);
                            } else if (event.getType() === ZooKeeper.Event.NODE_DATA_CHANGED
                                || event === ZooKeeper.Event.NODE_CREATED
                                || event === ZooKeeper.Event.NODE_CHILDREN_CHANGED) {
                                self.setchildcache(path, null);
                            }
                        },
                        function (error, children, stat) {
                            if (error) {
                                self.log('zk children get error: "%s", children =%s,', error, stat, children);
                                self.setchildcache(path, null);
                                reject(error);
                                return;
                            } else {
                                self.log('get zk children: ' + children);
                                self.setchildcache(path, children);
                                resolve(children);
                                return;
                            }
                        });
                }
            });
        });
    }

    delete(path) {
        let self = this;
        return new Promise(function (resolve, reject) {
            self.zookeeper.remove(path, -1, function (error) {
                if (error) {
                    let rc = error.getCode();
                    self.log('zk delete result: %d, error: "%s"', rc, error);
                    self.setchildcache(path, null);
                    reject(error);
                    return;
                } else {
                    self.log('zk delete suc');
                    resolve('');
                    return;
                }
            });
        });
    }
}

exports = module.exports = new ZKConfig();