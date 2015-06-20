var express = require('express');
var app = express();
var expressHbs = require('express-handlebars');
var serveStatic = require('serve-static');

// New call to compress content
app.use(serveStatic('public'));

app.engine('hbs', expressHbs({extname:'hbs', defaultLayout:'admin.hbs'}));
app.set('view engine', 'hbs');

app.get('/', function(req, res){
    var data = {numTests: 5};
    data.dashboard_title = 'RFX - AB Testing';

    res.render('summary', data);
});

app.get('/loop', function(req, res){
    var basketballPlayers = [
        {name: 'Lebron James', team: 'the Heat'},
        {name: 'Kevin Durant', team: 'the Thunder'},
        {name: 'Kobe Jordan',  team: 'the Lakers'}
    ];

    var days = [
        'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'
    ];

    var data = {
        basketballPlayers: basketballPlayers,
        days: days
    };

    res.render('loop', data);
});

app.get('/logic', function(req, res){
    var data = {
        upIsUp: true,
        downIsUp: false,
        skyIsBlue: "yes",
        someValue : "aaa"
    };

    res.render('logic', data);
});

app.listen(8181);
