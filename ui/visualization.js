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
var DEFAULT_PROPERTIES = [['Avg. Temperature', '&#8451;','avg-temp'], ['Avg. Wind speed', 'm/s', 'avg-wind-speed']];

var map = L.map('mapid').setView(INITIAL_CENTER, INITIAL_ZOOM_LEVEL);
var properties = DEFAULT_PROPERTIES;

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
        var value = Number(station[property[2]]).toFixed(2);
	
	return "<b>" + name + "</b> : " + value + " " + unit + "<br />";
}

function generatePopupMessage(station) {
	var result = "";
	for (var property in properties) {
		property = properties[property];
		result += propertyToText(station, property);
	}

	return result;

}

function plotStation(station) {
                var latitude = station['latitude']/1000;
                var longitude = station['longitude']/1000;
                
		var marker = L.marker([longitude,  latitude]).addTo(map);
		var popupMessage = generatePopupMessage(station)
                marker.bindPopup(popupMessage);
}

function plotStations(json) {
	var max = 0;
	for (var key in json) {
		var station = json[key];
		plotStation(station);
		// Max is just for testing to ensure not to many stations are loaded
		max++;
		if (max>100) {
			break;
		}
	}
}

loadWeatherStations(2015, 5, plotStations);
$('.ui-control').load('control-panel.html'); // Loads control menu content
