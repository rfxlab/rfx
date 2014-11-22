(function() {
    var Sock = function() {
		var socket;
		if (!window.WebSocket) {
			window.WebSocket = window.MozWebSocket;
		}

		if (window.WebSocket) {
			socket = new WebSocket("ws://127.0.0.1:8081/websocket");
			socket.onopen = onopen;
			socket.onmessage = onmessage;
			socket.onclose = onclose;
		} else {
			alert("Your browser does not support Web Socket.");
		}

		function onopen(event) {
			console.log("Web Socket opened!");
		}

		function onmessage(event) {			
			websocketMonitorHandler(event.data);
		}		

		function onclose(event) {
			console.log("Web Socket closed");
		}

		function numberWithCommas(x) {
		    return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
		}

		function websocketMonitorHandler(data){
			//console.log("websocketMonitorHandler: \n "+data);
			var obj = JSON.parse(data);
			if(obj['pageview']){
				jQuery('#monitor-pageview').html(obj['pageview']);
			} 
			if(obj['trending-urls']){
				$("ol#update").prepend('<li>'+data+'</li>');
				$("ol#update li:first").slideDown("slow");
				if($("ol#update li").size() > 5) {
					$("ol#update li:last").remove();
				}
			}
			
			//workerInfoHandler(obj);
			
//			for(var k in obj){
//				if(k.indexOf('total')===0)
//					$('#'+k).text( numberWithCommas(obj[k]) );
//				else 
//					$('#'+k).text( obj[k] );
//			}			
		}
		
    }
    window.addEventListener('load', function() {new Sock();}, false);
})();