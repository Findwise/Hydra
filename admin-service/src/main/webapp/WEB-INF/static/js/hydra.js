window.model = {};
window.templates = {};



$(document).ready(
	function() {
                
	$('.template').each(
		function(i, container) {
			window.templates[container.id] = Handlebars.compile($(
					container).html());
		});
	$('.partial_template').each(
		function(i, template) {
			Handlebars.registerPartial(template.id, $(template).html());
		});

	$("#navigation a").click(
		function() {
			var containerId = this.href.substring(this.href
					.lastIndexOf('#') + 1);
			refreshPage(containerId);
			showPage(containerId);
		});
        Handlebars.registerHelper('listDocuments', listDocuments);
        Handlebars.registerHelper('msToReadable', msToReadable);
        Handlebars.registerHelper('propertyField', propertyField);
        Handlebars.registerHelper('stageModeLabel', stageModeLabel);
        Handlebars.registerHelper('prettyPrint', prettyPrint);
        Handlebars.registerHelper('printProperties', printProperties);
	showCurrentPage();
	refreshCurrentPage();
});
