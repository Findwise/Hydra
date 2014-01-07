var pages = {
	'status': '/hydra',
	'stagegroups': '/hydra/stagegroups',
	'libraries' : '/hydra/libraries',
	'documents' : '/hydra/documents',
        'upload' : '/hydra/libraries'
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
                                        library.stages[stage].libId = library.id;
				}
                                
			});
			return data;
		};
	}
        
        else {
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

function queryDocuments() {
	var query = $("#documents_query_textarea").val();
	$.getJSON(pages.documents + "?q=" + query,
		function(data) {
			console.log(data);
			$('#documents_list').html(window.templates['documents_list_items'](data));
		}).fail(
		function(obj, textStatus, errorThrown) {
			alert("Query failed : " + errorThrown);
		});
}

function addDocument() {
	var stringDocBody = $("#documents_add_textarea").val();
	var docBody = $.parseJSON(stringDocBody);
	console.log(docBody);
	$.post(pages.documents + "/new", docBody,
		function(data) {
			console.log(data);
			queryDocuments();
		}).fail(
		function(obj, textStatus, errorThrown) {
			alert("Query failed : " + errorThrown);
		});
}

function addLibrary(){
        var formData = new FormData($('#library-upload')[0]);
        var libid = $('#upload-lib-id').val();
        if(libid){
            var postUrl = pages.upload + '/' + libid;
            $.ajax({
                url: postUrl,  //Server script to process data
                type: 'POST',
                xhr: function() {  // Custom XMLHttpRequest
                    var myXhr = $.ajaxSettings.xhr();
                    if(myXhr.upload){ // Check if upload property exists
                        myXhr.upload.addEventListener('progress',progressHandlingFunction, false); // For handling the progress of the upload
                    }
                    return myXhr;
                },
                //Ajax events
               success: function(data){
                   //should properly progagete data back to model
                   $('#libUploadSuccess').show();
               },
               error: function(data){
                   //should properly progagete data back to model
                   $('#libUploadFail').show();

               },
                // Form data
                data: formData,
                //Options to tell jQuery not to process data or worry about content-type.
                cache: false,
                contentType: false,
                processData: false
            });
            return false;
        } else {
           $('#libUploadFail').show();
        }
}

function progressHandlingFunction(e){
    if(e.lengthComputable){
        $('#jarupload').attr({value:e.loaded,max:e.total});
    }
}

function addStage(config){
    var stage_config = {};
    $.each(config,function(index,item){
       var key = $(this).attr('id');
       var value = $(this).val();
       if(value != ''){
           try{
            stage_config[key]=JSON.parse(value);
        } catch(e){
            stage_config[key]=value;
        }
       }
    });
    var jsonData = JSON.stringify(stage_config);
    console.log(stage_config);
    console.log(jsonData);
    //var jsonData = stage_config;
    var postUrl = pages.upload + '/' + stage_config.libId + '/stages/';
    if(stage_config.stageGroup){
        postUrl = postUrl + stage_config.stageGroup;
    } else {
        postUrl = postUrl + stage_config.stageName;
    }
    postUrl = postUrl + '/' + stage_config.stageName;
    $.ajax({
        type: "POST",
        url: postUrl,
        data: JSON.stringify(stage_config),
        contentType: "application/json; charset=utf-8",
        dataType: "json",
        success: function(data){
            console.log('Hurray');
        },
        error: function(data){
            console.log(data);
        }
    });
    
    return false;
}

function propertyField(property, name){
    var ret = "";
    if(this.type == 'Map' || this.type=='LocalQuery'){
        ret = ret + '<textarea name="' + name + '" class="form-control" row="3"  id="' + name +'" placeholder="' + this.type + '"';
        if(this.required){
            ret = ret + ' required';
        }
        ret = ret + '></textarea>';
    }
    else {
        ret = '<input type="text" name="' + name + '" class="form-control" id="' + name + '" placeholder="' + this.type + '"';
        if(this.required){
            ret = ret + ' required';
        }
        ret = ret + ' />';
    }
    var safeRet = new Handlebars.SafeString(ret);
    return safeRet;
}

function listDocuments(context, options) {
	var ret = "";

	for(var i = 0, j=context.length; i<j; i++) {
		ret = ret + "<div class='well'>";
		ret = ret + "<pre>" + JSON.stringify(context[i], null, '\t') + "</pre>";
		ret = ret + "</div>";
	}
	return ret;
}

function showCurrentPage() {
	var currentPage = getCurrentPage();
	if (currentPage !== "") {
		showPage(currentPage);
	}
}

function getCurrentPage() {
	var currentPage = window.location.hash.substring(1);
	if (currentPage === "") {
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
        Handlebars.registerHelper('propertyField', propertyField);
	showCurrentPage();
	refreshCurrentPage();
});

