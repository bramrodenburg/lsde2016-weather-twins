FIRST_YEAR = 1901;
LAST_YEAR = 2015;

MONTHS = {"January":1, "February":2, "March":3, "April":4, "May":5, "June":6, "July": 7, "August": 8, "September": 9, "October":10, "November":11, "December":12};

function changeData(origin) {	
	var originYear = $("#origin-year").find(":checked").val();
	var originMonth = $("#origin-month").find(":checked").val();
	console.log(originYear);
	console.log(originMonth);
	loadWeatherStations(originYear, originMonth, plotStations);
}

function setInitialSelectedOption(field, optionName) {
        $(field).filter(function() {
                return $(this).text() == optionName;
        }).prop('selected', true);
}

function populateSelect(name, values) {
	// Ugly code: if dictionary (!= Array)
	if (values.constructor != Array) {
		$.each(values, function(key, value) {
			$(name).append($("<option>", {value: value}).text(key));
		});
	} else {
        	$.each(values, function(key, value) {
                	$(name).append($("<option>", {value: value}).text(value));
        	});
	}
}

function getYears(start, end) {
	result = new Array(end+1-start);
	
	for (var i=0; i<=end-start; i++) {
        	result[i] = i+1901;
	}

	return result;
}

function populateControlPanel() {
	years = getYears(FIRST_YEAR, LAST_YEAR);
	populateSelect("#origin-year", years);
	populateSelect("#origin-month", MONTHS);
	populateSelect("#target-year", years);
	populateSelect("#target-month", MONTHS);

	setInitialSelectedOption("#origin-year option", DEFAULT_ORIGIN_YEAR);
	setInitialSelectedOption("#origin-month option", DEFAULT_ORIGIN_MONTH);
	setInitialSelectedOption("#target-year option", DEFAULT_TARGET_YEAR);
	setInitialSelectedOption("#target-month option", DEFAULT_TARGET_MONTH);

	$("#origin-year").on('change', changeData);
	$("#origin-month").on('change', changeData);
	//$("#origin-month").on('change', function() {alert($(this).find(":checked").val());});
	//$("#target-year").on('change', function() {alert($(this).find(":checked").val());});
	//$("#target-month").on('change', function() {alert($(this).find(":checked").val());});
}

populateControlPanel();
