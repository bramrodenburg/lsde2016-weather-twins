function evaluate() {
	$('#result').empty();
    var temp = document.getElementById("temperature").value;// get parameters from temperature and windspeed field
	var wind = document.getElementById("windspeed").value;
	var count;
	for(var i=1991; i<1993;i++){// the variable i loops through year and has to start with starting year available
		for(var j=1; j<13;j++)	{//j goes through moths
		example = i+"-"+j+".json";	//loops through the available files
			$.ajax({
			url: example,
			dataType: 'json',
			success: function(data){
			var distance;
			var array= data;
			for(var z=0; z < array.length; z++){//search for closest station
				distance = Math.sqrt(Math.pow(temp - array[z].temperature, 2) + Math.pow(wind - array[z].windspeed, 2));//euclidean distance
					if(distance < 0.1){
						var text= array[z].city+"\n in\n "+i+"-"+j;//creates the variable text to display on the map
						var appendItem ='<ul>Info:'+text+'<li>Longitude:'+array[z].longitude+'</li><li>Latitude:'+array[z].latitude+'</li><li>Temperature:'+array[z].temperature+'</li><li>Windspeed:'+array[z].windspeed+'</li></ul><a id="map-btn"; class="button" href="http://www.google.com/maps/place/'+ array[z].latitude +','+ array[z].longitude+'"; target="_blank";>GoogleMaps</a>';//call to googlemaps
						
						  $('#result').append(appendItem);
						  addpoint(array[z].longitude,array[z].latitude,text)//send points to funtipon that plots points on the map
						 
							
						
					}
			
				}
			
			},
			async:false
			});	
		}
	}	
	}
	
	function addpoint(lat,lon,text) {

  var gpoint = g.append("g").attr("class", "gpoint");
  var x = projection([lat,lon])[0];
  var y = projection([lat,lon])[1];

  gpoint.append("svg:circle")
        .attr("cx", x)
        .attr("cy", y)
        .attr("class","point")
		.style("stroke","green")
        .attr("r", 1.5);

  //conditional in case a point has no associated text
  if(text.length>0){

    gpoint.append("text")
          .attr("x", x+2)
          .attr("y", y+2)
          .attr("class","text")
          .text(text);
  }

}

	
	
