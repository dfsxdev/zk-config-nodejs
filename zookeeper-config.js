const ZooKeeper = require('zookeeper');
const Promise = require('bluebird');
//const _ = require('lodash');

global.ConfigSetFlag = {
    CREATE: 1,
    CREATE_TEMPORARY: 2,
    OVERWRITE: 3,
    OVERWRITE_IF_EXISTS: 4,
    SEQUENTIAL: 5,
    SEQUENTIAL_TEMPORARY: 6
};

let cacheflag = {
    cacheflag_init: 0,
    cacheflag_deleted: 1,
    cacheflag_created: 2,
    cacheflag_changedd: 2,
}

class ZKConfig {
    constructor() {
        let node_env = process.env.NODE_ENV ? process.env.NODE_ENV : 'development';
        let connect = node_env === 'development' ? '192.168.6.206:2181' : '192.168.6.206:2181';
        let timeout = 60000; // 单位毫秒
        let debug_level = ZooKeeper.ZOO_LOG_LEVEL_WARN;
        let host_order_deterministic = false;

        this.defaultInitOpt = {
            connect,
            timeout,
            debug_level,
            host_order_deterministic
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
        this.opt = opt;
        this._initZook();
        let self = this;
        this.cachedata = {};
        this.childcachedata = {};
        // return new Promise((resolve, reject) => {
        //     self.zookeeper.connect(function (error) {
        //         if (error) {
        //             reject(error);
        //             return;
        //         } else {
        //             console.log('zk session established, id=%s', self.zookeeper.client_id);
        //             resolve("connected");
        //             return;
        //         }
        //     });
        // });

        self.zookeeper.connect(function (error) {
                if (error) {
                    console.log('connect error, error=%s', error);
                } else {
                    console.log('zk session established, id=%s', self.zookeeper.client_id);
                }
            });
    }

    _initZook() {
        this.defaultInitOpt.connect = this.opt.connect;
        this.defaultInitOpt.timeout = this.opt.timeout;
        this.zookeeper = new ZooKeeper(this.defaultInitOpt);
    }

    setcache(path, value, flag = cacheflag.cacheflag_changedd) {
        if (this.cachedata[path] == null)
            this.cachedata[path] = {};
        this.cachedata[path].flag = flag;
        this.cachedata[path].data = value;
    }

    getcache(path) {
        return this.cachedata[path];
    }

    setchildcache(path, value, flag = cacheflag.cacheflag_changedd) {
        if (this.childcachedata[path] == null)
            this.childcachedata[path] = {};
        this.childcachedata[path].flag = flag;
        this.childcachedata[path].data = value;
    }

    getchildcache(path) {
        return this.childcachedata[path];
    }

    getzookflag(flag) {
        var zookflag = "";
        switch (flag) {
            case ConfigSetFlag.CREATE:
                zookflag = ZooKeeper.ZOO_PERSISTENT;
                break;
            case ConfigSetFlag.CREATE_TEMPORARY:
                zookflag = ZooKeeper.ZOO_EPHEMERAL;
                break;
            case ConfigSetFlag.OVERWRITE:
                zookflag = ZooKeeper.ZOO_PERSISTENT;
                break;
            case ConfigSetFlag.OVERWRITE_IF_EXISTS:
                zookflag = ZooKeeper.ZOO_PERSISTENT;
                break;
            case ConfigSetFlag.SEQUENTIAL:
                zookflag = ZooKeeper.ZOO_PERSISTENT | ZooKeeper.ZOO_SEQUENCE;
                break;
            case ConfigSetFlag.SEQUENTIAL_TEMPORARY:
                zookflag = ZooKeeper.ZOO_EPHEMERAL | ZooKeeper.ZOO_SEQUENCE;
                break;
            default:
                break;
        }

        return zookflag;
    }

    exists(path) {
        return new Promise((resolve, reject) => {
            let self = this;
            self.zookeeper.a_exists(path, null, function (rc, error, stat) {
                if (rc !== 0) {
                    console.log("node not exists, result: %d, error: '%s', stat=%j", rc, error, stat);
                    reject(error);
                    return;
                } else {
                    console.log('node exists!');
                    resolve(stat);
                    return;
                }
            });
        });
    }

    get(path) {
        return new Promise((resolve, reject) => {
            let self = this;
            let cache = self.getcache(path);
            if (cache != null) {
                if (cache.flag === cacheflag.cacheflag_deleted) {
                    self.setcache(path, null, cacheflag.cacheflag_init);
                } else if (cache.flag === cacheflag.cacheflag_init) {
                    console.log('get cache node: node deleted');
                    reject('node not exists');
                    return;
                } else if (cache.data != null) {
                    let data = cache.data;
                    console.log('get cache node: ' + data);
                    resolve(data);
                    return;
                }
            }

            self.zookeeper.aw_exists(path,
                function (type, state, path) {
                    console.log("get:aw_exists get watcher is triggered: type=%d, state=%d, path=%s", type, state, path);
                    if (type == ZooKeeper.ZOO_CREATED_EVENT) {
                        console.log("node created");
                        self.setcache(path, null);
                    }
                },
                function (rc, error, stat) {
                    if (rc != 0) {
                        console.log("node not exists, result: %d, error: '%s', stat=%j", rc, error, stat);
                        if (rc == ZooKeeper.ZNONODE) {
                            self.setcache(path, null, cacheflag.cacheflag_deleted);
                            reject('node not exists');
                        } else {
                            reject(error);
                        }
                        return;
                    } else {
                        self.zookeeper.aw_get(path,
                            function (type, state, path) {
                                console.log("get:: get watcher is triggered: type=%d, state=%d, path=%s", type, state, path);
                                if (type == ZooKeeper.ZOO_DELETED_EVENT) {
                                    console.log("node deleted");
                                    self.setcache(path, null, cacheflag.cacheflag_deleted);
                                } else if (type == ZooKeeper.ZOO_CHANGED_EVENT || type == ZooKeeper.ZOO_CREATED_EVENT) {
                                    self.setcache(path, null);
                                }
                            },
                            function (rc, error, stat, data) {
                                if (rc !== 0) {
                                    console.log('zk node get result: %d, error: "%s", stat=%s, data=%s', rc, error, stat, data);
                                    reject(error);
                                    return;
                                } else {
                                    console.log('get zk node: ' + data);
                                    //console.log('stat: ', stat);
                                    self.setcache(path, data);
                                    resolve(data);
                                    return;
                                }
                            });
                    }
                });
        });
    }

    overwrite_if_exists(in_path, value, flag, resolve, reject) {
        let self = this;

        var zookflag = self.getzookflag(flag);
        self.zookeeper.a_create(in_path, value, zookflag, function (rc, error, path) {
            if (rc !== 0) {
                if ((ZooKeeper.ZSYSTEMERROR < rc && rc < ZooKeeper.ZAPIERROR)
                    || rc == ZooKeeper.ZNOAUTH
                    || rc == ZooKeeper.ZSESSIONEXPIRED
                    || rc == ZooKeeper.ZAUTHFAILED
                    || rc == ZooKeeper.ZCLOSING
                    || rc == ZooKeeper.ZNONODE) {
                    console.log('zk node create result: %d, error: "%s", path=%s', rc, error, path);

                    reject(error);
                    return;
                }
                else {
                    console.log('create failed, result: %d, error: "%s", path=%s', rc, error, path);
                    self.zookeeper.a_set(in_path, value, -1, function (rc, error, stat) {
                        if (rc !== 0) {
                            if ((ZooKeeper.ZSYSTEMERROR < rc && rc < ZooKeeper.ZAPIERROR)
                                || rc == ZooKeeper.ZNOAUTH
                                || rc == ZooKeeper.ZSESSIONEXPIRED
                                || rc == ZooKeeper.ZAUTHFAILED
                                || rc == ZooKeeper.ZCLOSING) {
                                console.log('zk node set, other error, result: %d, error: "%s", stat=%s', rc, error, stat);

                                reject(error);
                                return;
                            }
                            else {
                                console.log('zk node set, continue , result: %d, error: "%s", stat=%s', rc, error, stat);
                                self.overwrite_if_exists(in_path, value, flag, resolve, reject);
                            }
                        } else {
                            console.log('set zk node succ!');

                            resolve(in_path);
                            return;
                        }
                    });
                }
            } else {
                console.log('create zk node succ! path=' + path);
                resolve(path);
                return;
            }
        });
    }

    set(path, value, flag) {
        let zkData = null;
        let self = this;
        return new Promise((resolve, reject) => {
                self.setcache(path, null);

                if (flag == ConfigSetFlag.OVERWRITE_IF_EXISTS) {
                    self.overwrite_if_exists(path, value, flag, resolve, reject);
                } else {
                    if (flag == ConfigSetFlag.CREATE || flag == ConfigSetFlag.CREATE_TEMPORARY
                        || flag == ConfigSetFlag.SEQUENTIAL || flag == ConfigSetFlag.SEQUENTIAL_TEMPORARY) {
                        var zookflag = self.getzookflag(flag);
                        self.zookeeper.a_create(path, value, zookflag, function (rc, error, path) {
                            if (rc !== 0) {
                                console.log('zk node create result: %d, error: "%s", path=%s', rc, error, path);

                                reject(error);
                                return;
                            } else {
                                console.log('create zk node succ! path=' + path);

                                resolve(path);
                                return;
                            }
                        });
                    } else if (flag == ConfigSetFlag.OVERWRITE) {
                        self.zookeeper.a_set(path, value, -1, function (rc, error, stat) {
                            if (rc !== 0) {
                                console.log('zk node set result: %d, error: "%s", stat=%s', rc, error, stat);

                                reject(error);
                                return;
                            } else {
                                console.log('set zk node succ!');

                                resolve(path);
                                return;
                            }
                        });
                    }
                }
            }
        );
    }

    getChildPaths(path) {
        return new Promise((resolve, reject) => {
            let self = this;

            let cache = self.getchildcache(path);
            if (cache != null) {
                if (cache.flag === cacheflag.cacheflag_deleted) {
                    self.setchildcache(path, null, cacheflag.cacheflag_init);
                } else if (cache.flag === cacheflag.cacheflag_init) {
                    console.log('get cache node: node deleted');
                    resolve(null);
                    return;
                } else if (cache.data != null) {
                    let data = cache.data;
                    console.log('get cache node: ' + data);
                    resolve(data);
                    return;
                }
            }

            self.zookeeper.a_exists(path, null, function (rc, error, stat) {
                if (rc != 0) {
                    console.log("node not exists, result: %d, error: '%s', stat=%j", rc, error, stat);
                    if (rc == ZooKeeper.ZNONODE) {
                        self.setchildcache(path, null, cacheflag.cacheflag_deleted);
                        reject('node not exists');
                    } else {
                        reject(error);
                    }
                    return;

                } else {
                    self.zookeeper.aw_get_children(path,
                        function (type, state, path) {
                            console.log("getChildPaths :: get watcher is triggered: type=%d, state=%d, path=%s", type, state, path);
                            if (type == ZooKeeper.ZOO_DELETED_EVENT) {
                                self.setchildcache(path, null, cacheflag.cacheflag_deleted);
                            } else if (type == ZooKeeper.ZOO_CHANGED_EVENT || type == ZooKeeper.ZOO_CREATED_EVENT || type == ZooKeeper.ZOO_CHILD_EVENT) {
                                self.setchildcache(path, null);
                            }
                        },
                        function (rc, error, children) {
                            if (rc !== 0) {
                                console.log('zk children get result: %d, error: "%s", children =%s,', rc, error, stat, children);
                                self.setchildcache(path, null);
                                reject(error);
                                return;
                            } else {
                                console.log('get zk children: ' + children);
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
        return new Promise((resolve, reject) => {
            let self = this;

            self.zookeeper.a_delete_(path, -1, function (rc, error) {
                    if (rc !== 0) {
                        console.log('zk delete result: %d, error: "%s"', rc, error);
                        self.setchildcache(path, null);
                        reject(error);
                        return;
                    } else {
                        console.log('zk delete suc');
                        resolve("");
                        return;
                    }
                });
        });
    }
}

exports = module.exports = new ZKConfig();