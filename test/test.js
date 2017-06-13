var zkconfig = require('../zookeeper-config');
var opt = {
    // connect: "localhost:2181" // zk server的服务器地址和监听的端口号
    connect: 'lstv-db:2181',
    timeout: 10000 // 以毫秒为单位
};

zkconfig.init(opt);

testSet();
function testSet() {
    var path = '/test';
    var value = {
        'private_base_url': 'http://192.168.6.206:8001/',
        'public_base_url': 'http://192.168.6.206:8001/'
    };

    value = JSON.stringify(value);

    var flag = ConfigSetFlag.CREATE_TEMPORARY;
    zkconfig.set(path, value, flag).then(
        function (result) {
            console.log('set OK, result: ' + result);

            path = result;
        },
        function (error) {
            console.log('set error:', error);
        }
    );
}

// zkconfig.delete("/test").then(
//     function (result) {
//         console.log("delete OK, result: " + result);
//     },
//     function (error) {
//         console.log("delete error:", error);
//     }
// );

setInterval(testGet, 10000);
//testGet();
function testGet() {
    var path = '/test';
    zkconfig.get(path).then(
        function (result) {
            console.log('get OK: ' + result);
        },
        function (error) {
            console.log('get error:', error);
        }
    );
}

// setInterval(testConnect, 5000);
function testConnect() {
    console.log(zkconfig.zookeeper);
}