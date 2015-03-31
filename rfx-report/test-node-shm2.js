//http://www.thegeekstuff.com/2008/11/overview-of-ramfs-and-tmpfs-on-linux/
// mkdir /home/trieunt/data/ramdata
// sudo mount -t tmpfs -o size=2048M tmpfs /home/trieu/data/ramdata

var mongoose = require('mongoose');
mongoose.connect('mongodb://localhost/test');
var express = require('express');
var app = express();

var memShm = require('mem-shm');
var memoryId = 'banner2key';

var bannerSchema = mongoose.Schema({
    bid : Number
} , { collection: 'banner'});

var BannerModel = mongoose.model('Banner', bannerSchema);

var shmFolderPath = "/home/trieu/data/ramdata/";

var redis = require("redis"),
    client = redis.createClient();

var mem = new memShm(shmFolderPath,"test-shm");
app.get('/get-with-shm', function (req, res) {

    //var tmp = mem.get();
    //console.log(tmp);

    var tmp = mem.get(memoryId,'1');
    //console.log(tmp);
    if(tmp != null){
        res.send(tmp+'');
    } else {
        BannerModel.find(function (err, banner) {
            if (!err) //
                var rs = banner[0].bid+'';// ...
            mem.set(memoryId,'1',rs);
            res.send(rs);
        });
    }
})

app.get('/get-no-shm', function (req, res) {
    BannerModel.find(function (err, banner) {
        var rs = banner[0].bid+'';// ...
        res.send(rs);
    });
})

app.get('/get-with-redis', function (req, res) {

    client.get("key1", function(err, rs) {
        // reply is null when the key is missing

        if(rs != null){
            res.send(rs.toString());
        } else {
            BannerModel.find(function (err, banner) {
                if (!err) {
                    var rs = banner[0].bid+'';// ...
                    client.set('key1', rs);
                    res.send(rs);
                }
            });
        }
    });

})

app.get('/clear-shm', function (req, res) {
    var mem = new memShm(shmFolderPath,"test-shm");
    var leng = mem.count();
    console.log('before '+leng);
    mem.set(memoryId,'1',null);
    mem.del(memoryId);
    leng = mem.count();
    console.log('after '+leng);
    res.send("clear "+leng);
});

app.get('/set1', function (req, res) {
    var mem = new memShm(shmFolderPath,"test-shm");
    mem.set(memoryId,'1',"1234567");
    res.send("set 1234567 ok ");
});

app.get('/set2', function (req, res) {
    var mem = new memShm(shmFolderPath,"test-shm");
    mem.set(memoryId,'1',"12345678");
    res.send("set 12345678 ok ");
});

app.get('/reload-shm', function (req, res) {
    mem = new memShm(shmFolderPath,"test-shm");
    res.send("reload-shm ok ");
});


var server = app.listen(3002, function () {
    var host = server.address().address;
    var port = server.address().port;
    console.log('Example app listening at http://%s:%s', host, port);
})