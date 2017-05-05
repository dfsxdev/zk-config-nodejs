//var zkconfig = require('zookeeper-config');
var zkconfig = require('./zookeeper-config/zookeeper-config');
var opt = {
    connect: "localhost:2181" // zk server的服务器地址和监听的端口号
    , timeout: 10000 // 以毫秒为单位
};
zkconfig.init(opt).then(
    function (data) {
        console.log("init OK");

        var path = "/test123/222/23232";
        var value = {
            key1: "123",
            key2: "333"
        };
        value = JSON.stringify(value);
        var flag = ConfigSetFlag.OVERWRITE;
        zkconfig.set(path, value, flag).then(
            function (result) {
                console.log("set OK");

                path = result;

                zkconfig.get(path).then(
                    function (result) {
                        console.log("get OK: " + result);

                        zkconfig.getChildPaths("/test123").then(
                            function (result) {
                                console.log("get2 OK: " + result);
                                //zkconfig.close();
                            },
                            function (error) {
                                console.log("get2 error:", error);
                            }
                        );
                    },
                    function (error) {
                        console.log("get error:", error);
                    }
                );
            },
            function (error) {
                console.log("set error:", error);
            }
        );
    },
    function (error) {
        console.log("init error:", error);
    }
);