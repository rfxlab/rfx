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
var mem = new memShm(shmFolderPath,"test-shm");

app.get('/', function (req, res) {

    var tmp = mem.get();
    console.log(tmp);

    var tmp = mem.get(memoryId,'1');
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

app.get('/ping', function (req, res) {
    res.send("PONG");
});

app.get('/reload-shm', function (req, res) {
    mem = new memShm(shmFolderPath,"test-shm");
    res.send("reload-shm ok ");
});

var server = app.listen(3001, function () {
    var host = server.address().address;
    var port = server.address().port;
    console.log('Example app listening at http://%s:%s', host, port);
})