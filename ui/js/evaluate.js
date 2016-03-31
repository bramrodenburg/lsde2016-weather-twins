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
						var text= array[z].station+"\n in\n "+i+"-"+j;//creates the variable text to display on the map
						var appendItem ='<ul>Info:\n in\n'+i+"-"+j+'<br><a>Station:'+array[z].station+'</a><br><a>Longitude:'+array[z].longitude+'</a><br><a>Latitude:'+array[z].latitude+'</a><br><a>Temperature:'+array[z].temperature+'</a><br><a>Windspeed:'+array[z].windspeed+'</a><br><a id="tit" class="btn-primary" href="http://www.google.com/maps/place/'+ array[z].latitude +','+ array[z].longitude+'"; target="_blank";>GoogleMaps</a></ul>';//call to googlemaps
						
						  $('#result').append(appendItem);
						  addpointo(array[z].longitude,array[z].latitude,text)//send points to funtipon that plots points on the map
						 
							
						
					}
			
				}
			
			},
			async:false
			});	
		}
	}	
	}
	
	function addpointo(lat,lon,text) {

  var gpoint = g.append("g").attr("class", "gpoint");
  var x = projection([lat,lon])[0];
  var y = projection([lat,lon])[1];

  
  
  
  gpoint.append("svg:circle")
        .attr("cx", x)
        .attr("cy", y)
        .attr("class","point")
		.style("fill","purple")
        .attr("r", 1.5);
		//.on('mouseover', function(){$("text1").show();});
		//.on('mouseout', function(){$("text1").hide();})

  //conditional in case a point has no associated text
  if(text.length>0){

    gpoint.append("text")
          .attr("x", x+2)
          .attr("y", y+2)
          .attr("class","text")
          .text(text);
  }

}

	
	
