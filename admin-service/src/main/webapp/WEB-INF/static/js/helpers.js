/* 
 * Copyright 2014 Magnus Ebbesson <magnus.ebbesson@findwise.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

function stageModeLabel(mode){
    if(mode === 'ACTIVE'){
        return new Handlebars.SafeString('<span class="label label-success">Active</span>');
    } else if(mode === 'DEBUG'){
        return new Handlebars.SafeString('<span class="label label-warning">Debug</span>');
    } else {
        return new Handlebars.SafeString('<span class="label label-default">Unknown</span>');
    }
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