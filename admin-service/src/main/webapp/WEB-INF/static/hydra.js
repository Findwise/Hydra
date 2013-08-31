var pages = {
	'status': '/hydra',
	'stagegroups': '/hydra/stagegroups',
	'libraries' : '/hydra/libraries',
	'documents' : '/hydra/documents'
};

function refreshAll() {
	for (var key in pages) {
		refreshPage(pages[key], key);
	}
}

function refreshCurrentPage() {
	refreshPage(getCurrentPage());
}

function refreshPage(pageId) {
	var endpoint = pages[pageId];
	
	if (pageId == "libraries") {
		callback = function(data) {
			data.libraries.forEach(function(library) {
				for (var stage in library.stages) {
					library.stages[stage].name = stage.split('.').pop();
					library.stages[stage].className = stage;
				}
			});
			return data;
		};
	} else {
		callback = function(data) {
			return data;
		};
	}

	$.get(endpoint, function(data) {
		data = callback(data);
		$('#' + pageId + '_content').html(window.templates[pageId](data));
	});
}

function msToReadable(ms) {
	x = ms / 1000;
	seconds = Math.round(x % 60);
	x /= 60;
	minutes = Math.round(x % 60);
	x /= 60;
	hours = Math.round(x % 24);
	x /= 24;
	days = Math.round(x);

	var res = "";
	if (days > 0)
		res += days + " day" + (days != 1 ? "s" : "") + ", ";
	if (hours > 0)
		res += hours + " hour" + (hours != 1 ? "s" : "") + ", ";
	if (minutes > 0)
		res += minutes + " minute" + (minutes != 1 ? "s" : "") + " and ";
	return res + seconds + " second" + (seconds != 1 ? "s" : "");
}

function startSource(name) {
	$.post("api/start?source=" + name, function(data) {
		console.log(data);
		refreshCurrentPage();
	});
}

function stopSource(name) {
	$.post("api/stop?source=" + name, function(data) {
		console.log(data);
		refreshCurrentPage();
	});
}

function startDispatcher(name) {
	$.post("api/start?dispatcher=" + name, function(data) {
		console.log(data);
		refreshCurrentPage();
	});
}

function stopDispatcher(name) {
	$.post("api/stop?dispatcher=" + name, function(data) {
		console.log(data);
		refreshCurrentPage();
	}).fail(function(obj, textStatus, errorThrown) {
		alert("Failed to stop dispatcher '" + name + "': " + errorThrown);
	});
}

function showCurrentPage() {
	var currentPage = getCurrentPage();
	if (currentPage != "") {
		showPage(currentPage);
	}
}

function getCurrentPage() {
	var currentPage = window.location.hash.substring(1);
	if (currentPage == "") {
		// Try finding the navigation element with class active and use that
		currentPage = $("#navigation li.active a").attr("href").substring(1);
	}
	return currentPage;
}

function showPage(containerId) {
	$("#navigation li").removeClass("active");
	$("main article.container").css("display", "none");

	$("#navigation li a[href=#" + containerId + "]").parent("li").addClass(
			"active");
	$("#" + containerId + "_content").css("display", "block");
}

window.templates = {};

$(document).ready(
	function() {
		$('.template').each(
				function(i, container) {
					window.templates[container.id] = Handlebars.compile($(
							container).html());
				});
		$('.partial_template').each(function(i, template) {
			Handlebars.registerPartial(template.id, $(template).html());
		});

		$("#navigation a").click(
			function() {
				var containerId = this.href.substring(this.href
						.lastIndexOf('#') + 1);
				refreshPage(containerId);
				showPage(containerId);
			});
	showCurrentPage();
	refreshCurrentPage();
});