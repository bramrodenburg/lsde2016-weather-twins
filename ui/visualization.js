// MapBox settings
var ACCESS_TOKEN = "pk.eyJ1IjoiYnJhbXJvZGVuYnVyZyIsImEiOiJjaW1nZ3pnajAwMDdtdzdtM2E3Z3FsN256In0.JSV2ucphP4xFzw1VxnqOdA";
var MAP_ID = "bramrodenburg.pia868gp";
// Default UI settings for LeafLet
var INITIAL_ZOOM_LEVEL = 3;
var INITIAL_CENTER = [51.505, -0.09];
var MAX_ZOOM_LEVEL = 18;
var MAP_ATTRIBUTION = "";
var DEFAULT_ORIGIN_YEAR = DEFAULT_TARGET_YEAR = 2015;
var DEFAULT_ORIGIN_MONTH = "May";
var DEFAULT_TARGET_MONTH = "June";
var DEFAULT_PROPERTIES = [['Avg. Temperature', '&#8451;','avg-temp', 2], ['Avg. Wind speed', 'm/s', 'avg-wind-speed', 2], ['Avg. Cloud height', 'km', 'avg-sky', 3, 0.0001], ['Avg. Wind direction', '&deg;', 'avg-wind-direction', 1, 180/Math.PI, 180], ['Avg. Visibility', 'km', 'avg-visibility', 2, 0.0001]];

var map = L.map('mapid').setView(INITIAL_CENTER, INITIAL_ZOOM_LEVEL);
var properties = DEFAULT_PROPERTIES;
var markers = [];
var numberOfMarkers = 0;

L.tileLayer('https://api.tiles.mapbox.com/v4/{id}/{z}/{x}/{y}.png?access_token={accessToken}', {
	attribution: MAP_ATTRIBUTION,
	maxZoom: MAX_ZOOM_LEVEL,
	id: MAP_ID,
	accessToken: ACCESS_TOKEN,
}).addTo(map);   

var UIControl = L.Control.extend({
	options: {
		position: 'topright'
	},

	onAdd: function(map) {
		var container = L.DomUtil.create('div', 'ui-control');
		
		// Content of ui control is added later in the loading....

		return container;
	}
});

map.addControl(new UIControl());

var MainTitleControl = L.Control.extend({
	options: {
		position: 'bottomleft'
	},

	onAdd: function(map) {
		var container = L.DomUtil.create('div', 'main-title');
		
		container.innerHTML += "<h1>Find similar weather occurences throughout the world!</h1>";

		return container;
	}
});

map.addControl(new MainTitleControl());

var resetButton;

function resetMap() {
	clearMarkers();

	var originYear = Number($("#origin-year").find(":checked").val());
        var originMonth = Number($("#origin-month").find(":checked").val());	

	loadWeatherStations(originYear, originMonth, plotStations);
	resetButton.removeFrom(map);
}

function showResetButton() {
	var ResetControl = L.Control.extend({
		options: {
		position: 'bottomright'
	},

	onAdd: function(map) {
		var container = L.DomUtil.create('div', 'reset-button');

		container.innerHTML += "<a href=\"#\" onclick=\"resetMap()\"><h1>Click here to reset</h1></a>";

		return container;
	}});

	resetButton = new ResetControl();

	map.addControl(resetButton);
}

function euclideanDistance(x, y, properties) {
	squaredSum = 0;

	for (var property in properties) {
		property = properties[property];
		if (x.hasOwnProperty(property) && y.hasOwnProperty(property)) {
			squaredSum += Math.pow(x[property] - y[property], 2);
		} else {
			return Number.MAX_VALUE;
		}
	}
	return Math.sqrt(squaredSum);

}

function findSimilarWeatherStations(benchmarkStation, includedWeatherAttributes, targetYear, targetMonth) {
	var mostSimilarStation;
	loadWeatherStations(targetYear, targetMonth, function(json) {
		var shortestDistance = Number.MAX_VALUE;
		
		for (var key in json) {
                	var targetStation = json[key];
                	var distance = euclideanDistance(benchmarkStation, targetStation, includedWeatherAttributes);
                	
			if (distance < shortestDistance && targetStation['identifier']!=benchmarkStation['identifier'] 
				&& targetStation.hasOwnProperty('latitude') && targetStation.hasOwnProperty('longitude')) {
                	        mostSimilarStation = targetStation;
				shortestDistance = distance;
                	}
        	}

		if (shortestDistance == Number.MAX_VALUE) {
			window.alert("Unfortunately it was not possible to find a weather twin for this location. Please try a different location or a different combination of weather attributes.");
			return;
		}

		clearMarkers();

		// Code below adds two new markers that visualize the results
	        var marker = L.marker([benchmarkStation.longitude/1000,  benchmarkStation.latitude/1000], getAttributes(benchmarkStation)).addTo(map);
                var popupMessage = generatePopupMessage(benchmarkStation, numberOfMarkers, false)
                marker.bindPopup(popupMessage);
                markers.push(marker);
                numberOfMarkers++;

	        marker = L.marker([mostSimilarStation.longitude/1000,  mostSimilarStation.latitude/1000], getAttributes(mostSimilarStation)).addTo(map);
                popupMessage = generatePopupMessage(mostSimilarStation, numberOfMarkers, false);
                marker.bindPopup(popupMessage);
                markers.push(marker);
                numberOfMarkers++;
		// End of new marker visualization
		showResetButton();
	});
}

function getSelectedAttributes() {
	var results = [];

	for (var property in DEFAULT_PROPERTIES) {
		property = DEFAULT_PROPERTIES[property];
		if ($("#attributes-field #" + property[2]).is(':checked')) {
			results.push(property[2]);
		}
	}

	return results;
}

function findWeatherTwins(markerID) {
	var targetYear = Number($("#target-year").find(":checked").val());
	var targetMonth = Number($("#target-month").find(":checked").val());

	var includedWeatherAttributes = getSelectedAttributes();
	if (includedWeatherAttributes.length == 0) {
		window.alert("Please include at least one attribute for weather comparison.");
		return;
	}

	var originStation = markers[markerID].options.station;
	var similarWeatherStations = findSimilarWeatherStations(originStation, includedWeatherAttributes, targetYear, targetMonth);
}

function clearMarkers() {
	for (var marker in markers) {
		marker = markers[marker];
		map.removeLayer(marker);
	}
	markers = [];
	numberOfMarkers = 0;
}

function loadWeatherStations(year, month, callback) {
	var filePath = "data/" + year + "/" + year + "-" + month + ".json";
	var result;
	return $.getJSON(filePath, callback);
}

function propertyToText(station, property) {
	if (!station.hasOwnProperty(property[2])) { // Return nothing if station hasn't measured property
		return "";
	}

	if (station[property[2]] == "NaN") {
		return "";
	}

	var name = property[0];
        var unit = property[1];
	var numberOfDecimals = property[3];
	var scale = 1;
	var offset = 0;
	if (property.length>=5) { // Includes scaling factor
		scale = property[4];
	} 
	if (property.length>=6) {
		offset = property[5];
	}
        var value = Number(station[property[2]]*scale + offset).toFixed(numberOfDecimals);
	
	return "<b>" + name + "</b> : " + value + " " + unit + "<br />";
}

function generatePopupMessage(station, numberOfMarkers, showButton) {
	var result = "";
	for (var property in properties) {
		property = properties[property];
		result += propertyToText(station, property);
	}
	
	if (showButton==true) {
		result += '<button onClick="findWeatherTwins(' + numberOfMarkers + ')">Find weather twin!</button>';
	}

	return result;

}

function getAttributes(station) {
	result = {};

	for (var property in properties) {
		property = properties[property][2];
		if (station.hasOwnProperty(property) && property != "NaN") {
			result[property] = Number(station[property]);
		} 
	}

	result['identifier'] = station['identifier'];
	result['longitude'] = station['longitude'];
	result['latitude'] = station['latitude'];
	
	return {station: result};
}

function plotStation(station) {
                var latitude = station['latitude']/1000;
                var longitude = station['longitude']/1000;
                
		var marker = L.marker([longitude,  latitude], getAttributes(station)).addTo(map);
		var popupMessage = generatePopupMessage(station, numberOfMarkers, true)
                marker.bindPopup(popupMessage);
		markers.push(marker);
		numberOfMarkers++;
}

function plotStations(json) {
	clearMarkers();
	var max = 0;
	for (var key in json) {
		var station = json[key];
		plotStation(station);
		// Max is just for testing to ensure not to many stations are loaded
		max++;
		if (max>1000) {
			break;
		}
	}
}

loadWeatherStations(2015, 5, plotStations);
$('.ui-control').load('control-panel.html'); // Loads control menu content
