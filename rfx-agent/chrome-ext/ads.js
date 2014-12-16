//hack for demo
var points = [];
var pointKeys = {};

function sendHeatmapData(){
	if(points.length === 0) return;		
	var url = baseUrl + "/u?url=" + encodeURIComponent(currentUrl) + "&heats=";
    while (true) {
        var p = points.shift();
        //console.log(p);
        if (p) {            
            if(p.key) {
				//url += (p.x + "j" + p.y + "j" + p.value + "j" + p.key + "d");	
				delete pointKeys[p.key];
            } else {            	
            }
            url += (p.x + "j" + p.y + "j" + p.value + "d");	
        } else {
            break;
        }
    }   
    console.log("sendHeatmapData "+url);
	jQuery('body').append('<img src="'+url+'" width="0" width="0" />');    
}


function trackItem(title){
	var h = function(rs){			
		console.log(rs);
	};
	var data = {url : currentUrl, refer : referrerUrl, title : title }	
	var url = baseUrl + "/tk?" + encodeQueryData(data);
	console.log(url);
	jQuery('body').append('<img src="'+url+'" width="0" width="0" />');
}

function trackUserActivity(){
	var heatTrackHandler = function (e) {
        // we need preventDefault for the touchmove
        //e.preventDefault();
        var x = e.pageX;
        var y = e.pageY;		
        if (e.touches) {
            x = e.touches[0].pageX;
            y = e.touches[0].pageY;
        }
        var point = { x: x, y: y, value: 1 };
        points.push(point);
    };
	jQuery('body').mousemove(heatTrackHandler);	
	
	jQuery('body').mousedown(function (e) {       
        var x = e.pageX;
        var y = e.pageY;       
        var point = { x: x, y: y, value: 3 };
        console.log(point);
        points.push(point);
        sendHeatmapData();
    });

    jQuery('.product-card__img').mousemove(function (e) {           	            
        var imgSrc = jQuery(this).find('img').attr('src');
        var x = e.pageX;
        var y = e.pageY; 
        if( pointKeys[imgSrc] ) return;

        var point = { x: x, y: y, value: 2 , key : imgSrc };
        pointKeys[imgSrc] = true;        
        console.log(point);
        points.push(point);        
    });	

	//send heatmap data every 3 seconds
	setInterval(sendHeatmapData, 3000);
}

jQuery(window).on('beforeunload', function() {
     sendHeatmapData();
});

if(currentUrl.indexOf('lazada.vn') > 4 ){
	var divC = jQuery('#prd-detail-page').find('.l-sidebar__right');
	//divC.prepend(html);	
	
	var title = jQuery('#prod_title').text();	
	trackItem(title);
	trackUserActivity();
} 
else if(currentUrl.indexOf('amazon.com') > 4 ){
	var divC = jQuery('#wishlistButtonStack');
	//divC.append(html);	

	var title = jQuery('#productTitle').text();	
	trackItem(title);
	trackUserActivity();
}