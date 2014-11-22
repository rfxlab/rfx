// create the module and name it scotchApp
var dashboardApp = angular.module('dashboardApp', [ 'ngRoute' ]);

// configure our routes
dashboardApp.config(function($routeProvider) {
    $routeProvider

    // route for the home page
    .when('/', {
        templateUrl : 'html/pages/monitor.html',
        controller : 'monitorController'
    })

    .when('/monitor', {
        templateUrl : 'html/pages/monitor.html',
        controller : 'monitorController'
    })

    .when('/workers', {
        templateUrl : 'html/pages/workers.html',
        controller : 'workersController'
    })

    .when('/system-events', {
        templateUrl : 'html/pages/system-events.html',
        controller : 'systemEventsController'
    })

    .when('/deployment', {
        templateUrl : 'html/pages/deployment.html',
        controller : 'deploymentController'
    })

    .when('/settings', {
        templateUrl : 'html/pages/settings.html',
        controller : 'settingsController'
    })

    .when('/topologies', {
        templateUrl : 'html/pages/topologies.html',
        controller : 'topologiesController'
    })

    // route for the about page
    .when('/about', {
        templateUrl : 'html/pages/about.html',
        controller : 'aboutController'
    })

});

dashboardApp.controller('mainController', function($scope) {

});

// create the controller and inject Angular's $scope
dashboardApp.controller('initTabsController', function($scope) {
    $scope.sections = [ {
        key : 'monitor',
        name : 'Monitor'
    }, {
        key : 'workers',
        name : 'Workers'
    }, {
        key : 'system-events',
        name : 'System Events'
    }, {
        key : 'deployment',
        name : 'Deployment'
    }, {
        key : 'settings',
        name : 'Settings'
    }, {
        key : 'topologies',
        name : 'Topologies'
    }, {
        key : 'about',
        name : 'About'
    } ];

    $scope.setMaster = function(section) {
        $scope.selected = section;
    }

    $scope.isSelected = function(section) {
        return $scope.selected === section;
    }
    // console.log('setTimeout');
    setTimeout(function() {
        // console.log($('#tabs').html());
        // $('#tabs').find('li:first').addClass('active');
    }, 500);

});

dashboardApp.controller('monitorController', function($scope) {
    $scope.students = [ {
        name : 'Trieu',
        id : '1'
    }, {
        name : 'Tom',
        id : '2'
    }, {
        name : 'Jill Hill',
        id : '3'
    } ];
});

dashboardApp.controller('workersController', function($scope) {
	//do this 
	$scope.intervalFunction = function(){
	    $timeout(function() {
	    	loadClusterData()
	    	$scope.intervalFunction();
	    }, 1000)
	  };

	  // Kick off the interval
	  $scope.intervalFunction();
});

dashboardApp.controller('systemEventsController', function($scope) {

});

dashboardApp.controller('deploymentController', function($scope) {
    // do this first
    var fileInput = document.getElementById('fileInput');
    fileInput.addEventListener('change', function(e) {
        var file = fileInput.files[0];
        var lastModifiedDate = file.lastModifiedDate;
        var name = file.name;
        var size = file.size;
        var type = file.type;
        // console.log(file);
        var jarType = 'application/x-java-archive';

        if (type === jarType && size > 0) {
            var reader = new FileReader();
            reader.onload = function(e) {
                var fileData = reader.result;
                if (fileData) {
                    var fileBase64Data = fileData.substring(fileData
                            .indexOf('base64,') + 7);
                    var md5hash = SparkMD5.hash(fileBase64Data);
                    $.post("json/deploy-topology", {
                        base64data : fileBase64Data,
                        token : md5hash,
                        filename : name
                    }, function(data) {
                        alert(data);
                    });
                }
            }
            reader.readAsDataURL(file);
        } else {
            fileDisplayArea.innerText = "File not supported!"
        }
    });
});

dashboardApp.controller('settingsController', function($scope) {
    // do this
});

dashboardApp.controller('topologiesController', function($scope) {

});

dashboardApp.controller('aboutController', function($scope) {

});

// http://stackoverflow.com/questions/6978156/get-base64-encode-file-data-from-input-form

var workerInfoHandler = function(list) {
    $('#cluster_list').find('tr').remove();
    for ( var i in list) {
        // console.log(list[i]);
        var func = "restartNode(this, '" + list[i].worker_name + "','"
                + list[i].hostname + "');"
        var funcSetting = "showPopup('" + list[i].worker_name + "','"
                + list[i].partition + "','" + list[i].kafka_topic + "');"

        list[i].action = "<p><a  href=\"javascript:;\" ref=\"" + i
                + "\" onclick=\"" + funcSetting + "\"  >setting</a></p>";
        list[i].action += "<p><a href=\"javascript:;\" ref=\"" + i
                + "\" onclick=\"" + func + " return false;\" > restart</a></p>";
        var node = $('<tr id="row' + i + '"></tr>').loadTemplate(
                $("#template"), list[i]);
        var statusNode = node.find('span.status_dead');
        statusNode.attr('id', 'status' + i);
        if (statusNode.text() === 'ALIVE') {
            statusNode.attr('class', 'status_alive');
        }
        $('#cluster_list').append(node);
    }
};

function loadClusterData() {
    $('#cluster_list').slideUp();
    $.getJSON("json/get-workers", function(list) {
        workerInfoHandler(list);
        $('#cluster_list').slideDown();

        // setup worker tab
        // workerSetup.loadWorkerToList(list);
    });
}
