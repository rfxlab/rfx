function restartNode(e, worker, hostname){		
	var check = false;				
    var $this = $(e);		
	$this.attr("onclick", "");	
	$this.html("stopping...");
	
	var opts = {
		url : 'http://127.0.0.1:8080/ajax',
		action: 'kill',
		worker_name : worker,
		hostname: hostname
	};
	var uri = opts.url + "?worker_name=" + opts.worker_name + "&hostname=" + opts.hostname + "&action=" +opts.action;
      
	// command start              
    $.ajax(uri, {
            type: 'POST', 
            dataType: 'json',
            success: function(data) {
				if (data.status === 'ok')
				{
				    console.log(""+ opts.action +" is :" + data.status);
					    
				    var t1 = setTimeout(function(){

							// check command
			  	     	 	opts.action = 'check';
							$this.html(opts.action);
			      	        uri = opts.url + "?worker_name=" + opts.worker_name + "&hostname=" + opts.hostname + "&action=" +opts.action;
				      	     
				      	     // check 
				      	     $.ajax(uri, {
						    type: 'POST', 
						    dataType: 'json',
						    success: function(data) {
							if (data.status === 'ok')
							{
							    console.log(""+ opts.action +" is :" + data.status);
							    $this.html("checked");
							    
							    var index = $this.attr("ref");
							    var statusNode = $("#status" + index );
							    
							    statusNode.attr('class','status_' + data.msg);
							    statusNode.html(data.msg.toUpperCase());
							   // console.log(statusNode.html())

							}
							else {
							    alert(data.msg, "Lỗi");
							}
							clearTimeout(t1);
						  }
					      }); // ajax	

				    }, 5000);
				}
				else {
				    alert(data.msg, "Error");
				}
            }
	}); // ajax             
        
};

var workerInfoHandler = function(list) {
	$('#cluster_list').find('tr').remove();
	for(var i in list){
		//console.log(list[i]);
		var func = "restartNode(this, '" + list[i].worker_name  +"','" + list[i].hostname + "');"
		var funcSetting = "showPopup('" + list[i].worker_name  +"','" + list[i].partition + "','" + list[i].kafka_topic + "');"

		list[i].action = "<p><a  href=\"javascript:;\" ref=\""+ i + "\" onclick=\""+ funcSetting +"\"  >setting</a></p>";
		list[i].action += "<p><a href=\"javascript:;\" ref=\""+ i + "\" onclick=\""+ func +" return false;\" > restart</a></p>";
		var node = $('<tr id="row'+i+'"></tr>').loadTemplate($("#template"), list[i] );				
		var statusNode = node.find('span.status_dead');
		statusNode.attr('id','status' + i);
		if(statusNode.text() === 'ALIVE'){
			statusNode.attr('class','status_alive');
		}
		$('#cluster_list').append(node);
	}
}; 
		
function loadClusterData(){			
	$('#cluster_list').slideUp();	
	$.getJSON( "json/get-workers", function(list){
		workerInfoHandler(list);
		$('#cluster_list').slideDown();
		
		// setup worker tab
		workerSetup.loadWorkerToList(list);
	});
}

function loadSystemEvents() {
	$('#system_events_list').slideUp();
	$.getJSON("/json/get-system-logged-events", function(list){
		systemEventsHandler(list);
		$('#system_events_list').slideDown();
	});	
}

var systemEventsHandler = function(list) {
	$('#system_events_list').find('tr').remove();
	for(var i in list){
		var formattedDate = $.formatDateTime('mm/dd/yy hh:ii:ss', new Date(list[i].loggedTime));
		list[i].loggedTime = formattedDate;
		var node = $('<tr id="row'+i+'"></tr>').loadTemplate($("#template-system-events"), list[i] );				
		$('#system_events_list').append(node);
	}
}; 