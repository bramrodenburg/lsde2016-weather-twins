// MapBox settings
var ACCESS_TOKEN = "pk.eyJ1IjoiYnJhbXJvZGVuYnVyZyIsImEiOiJjaW1nZ3pnajAwMDdtdzdtM2E3Z3FsN256In0.JSV2ucphP4xFzw1VxnqOdA";
var MAP_ID = "bramrodenburg.pia868gp";
// Default UI settings
var INITIAL_ZOOM_LEVEL = 3;
var INITIAL_CENTER = [51.505, -0.09];
var MAX_ZOOM_LEVEL = 18;
var MAP_ATTRIBUTION = "";
var DEFAULT_ORIGIN_YEAR = DEFAULT_TARGET_YEAR = 2015;
var DEFAULT_ORIGIN_MONTH = 1;
var DEFAULT_TARGET_MONTH = 2;
var DEFAULT_PROPERTIES = [['Temperature', '&#8451;','avg-temp'], ['Wind speed', 'm/s', 'avg-wind-speed']];

var map = L.map('mapid').setView(INITIAL_CENTER, INITIAL_ZOOM_LEVEL);
var properties = DEFAULT_PROPERTIES;

L.tileLayer('https://api.tiles.mapbox.com/v4/{id}/{z}/{x}/{y}.png?access_token={accessToken}', {
	attribution: MAP_ATTRIBUTION,
	maxZoom: MAX_ZOOM_LEVEL,
	id: MAP_ID,
	accessToken: ACCESS_TOKEN,
}).addTo(map);   

function loadWeatherStations(year, month, callback) {
	var filePath = "data/" + year + "/" + year + "-" + month + ".json";
	var result;
	return $.getJSON(filePath, callback);
}

function generatePopupMessage(station) {
	var result = "";
	for (var property in properties) {
		property = properties[property];
		console.log(property[0]);
		var name = property[0];
		var unit = property[1];
		var value = Number(station[property[2]]).toFixed(2);
		result += "<b>" + name + "</b>:" + value + " " + unit + "<br />"
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
