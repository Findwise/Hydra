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

function propertyField(name, property) {

    var safeRet = new Handlebars.SafeString(propertyFieldHelper(name, property));
    return safeRet;
}

function propertyFieldHelper(name, property) {
    var ret = "";
    if (property.type == 'Map' || property.type == 'LocalQuery') {
        ret = ret + '<textarea name="' + name + '" class="form-control" row="3"  id="' + name + '" placeholder="' + property.type + '"';
        if (property.required) {
            ret = ret + ' required';
        }
        ret = ret + '>';
        if (property.value) {
            ret = ret + prettyPrint(property);
        }
        ret = ret + '</textarea>';
    }
    else {
        ret = '<input type="text" name="' + name + '" class="form-control" id="' + name + '" placeholder="' + property.type + '"';
        if (property.required) {
            ret = ret + ' required';
        }
        if (property.value) {
            ret = ret + ' value=' + JSON.stringify(property.value);
        }
        ret = ret + ' />';
    }
    return ret;
}

function listDocuments(context, options) {
    var ret = "";

    for (var i = 0, j = context.length; i < j; i++) {
        ret = ret + "<div class='well'>";
        ret = ret + "<pre>" + JSON.stringify(context[i], null, '\t') + "</pre>";
        ret = ret + "</div>";
    }
    return ret;
}

function stageModeLabel(mode) {
    if (mode === 'ACTIVE') {
        return new Handlebars.SafeString('<span class="label label-success">Active</span>');
    } else if (mode === 'DEBUG') {
        return new Handlebars.SafeString('<span class="label label-warning">Debug</span>');
    } else {
        return new Handlebars.SafeString('<span class="label label-default">Unknown</span>');
    }
}


function prettyPrint(value) {
    if (typeof value == 'string') {
        return new Handlebars.SafeString(value);
    } else if (typeof value == 'object') {
        if(value.value){
            return new Handlebars.SafeString(JSON.stringify(value.value));
        } else {
             return new Handlebars.SafeString('-');
        }
    } else {
        return new Handlebars.SafeString('-');
    }

}

function printProperties(key, value) {

    if (key == 'libId' || key == 'stageClass') {

        return new Handlebars.SafeString('<input type="hidden" id="' + key + '" value="' + value + '" />');
    } else if (key == 'stageName' && value) {
        return new Handlebars.SafeString('<input type="hidden" id="' + key + '" value="' + value + '" />');
    } else if (key == 'stageGroup' && value) {
        return new Handlebars.SafeString('<input type="hidden" id="' + key + '" value="' + value + '" />');
    }

    else {

        var ret = '<div class="form-group"><label class="col-sm-4 control-label" for="' + key + '">' + key;

        if (value.required) {
            ret = ret + '*';
        }
        ret = ret + '</label><div class="col-sm-8">';
        ret = ret + propertyFieldHelper(key, value);
        ret = ret + '<span class="help-block">' + this.description + '</span></div></div>';
        return  new Handlebars.SafeString(ret);
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