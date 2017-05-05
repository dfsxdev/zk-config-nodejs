//var zkconfig = require('zookeeper-config');
var zkconfig = require('../zookeeper-config/zookeeper-config');
var opt = {
    connect: "localhost:2181" // zk server的服务器地址和监听的端口号
    //connect: "192.168.6.206:2181"
    , timeout: 10000 // 以毫秒为单位
};

zkconfig.init(opt);

setTimeout(test_timeout, 20000);

var path = "/nmip/api/nodes/general";
var value = {
    private_base_url: "http://192.168.6.206:8001/",
    public_base_url: "http://192.168.6.206:8001/"
};

path = "/nmip/api/nodes/live";
value = {
    private_base_url: "http://192.168.6.206:8101/",
    public_base_url: "http://192.168.6.206:8101/"
};

value = JSON.stringify(value);
var flag = ConfigSetFlag.OVERWRITE_IF_EXISTS;
zkconfig.set(path, value, flag).then(
    function (result) {
        console.log("set OK, result: " + result);

        path = result;
    },
    function (error) {
        console.log("set error:", error);
    }
);

zkconfig.delete("/test123").then(
    function (result) {
        console.log("delete OK, result: " + result);
    },
    function (error) {
        console.log("delete error:", error);
    }
);

function test_timeout() {
    console.log("init OK 2");

    var path = "/test123";
    var value = {
        key1: "111",
        key2: "222"
    };
    value = JSON.stringify(value);
    var flag = ConfigSetFlag.OVERWRITE_IF_EXISTS;

    zkconfig.get(path).then(
        function (result) {
            console.log("get OK: " + result);

            zkconfig.get(path).then(
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
}