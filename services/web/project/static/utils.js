////////////////////////////////////////////////////////////////////////////////
// url functions
////////////////////////////////////////////////////////////////////////////////

// see: https://stackoverflow.com/questions/901115/how-can-i-get-query-string-values-in-javascript
function getParameterByName(name, url = window.location.href) {
    name = name.replace(/[\[\]]/g, '\\$&');
    var regex = new RegExp('[?&]' + name + '(=([^&#]*)|&|#|$)'),
        results = regex.exec(url);
    if (!results) return null;
    if (!results[2]) return '';
    return decodeURIComponent(results[2].replace(/\+/g, ' '));
}

// see: https://stackoverflow.com/questions/1634748/how-can-i-delete-a-query-string-parameter-in-javascript
function removeURLParameter(url, parameter) {
    //prefer to use l.search if you have a location/link object
    var urlparts = url.split('?');
    if (urlparts.length >= 2) {

        var prefix = encodeURIComponent(parameter) + '=';
        var pars = urlparts[1].split(/[&;]/g);

        //reverse iteration as may be destructive
        for (var i = pars.length; i-- > 0;) {
            //idiom for string.startsWith
            if (pars[i].lastIndexOf(prefix, 0) !== -1) {
                pars.splice(i, 1);
            }
        }

        return urlparts[0] + (pars.length > 0 ? '?' + pars.join('&') : '');
    }
    return url;
}

// see: https://stackoverflow.com/questions/1090948/change-url-parameters
function updateURLParameter(url, param, paramVal)
{
    var TheAnchor = null;
    var newAdditionalURL = "";
    var tempArray = url.split("?");
    var baseURL = tempArray[0];
    var additionalURL = tempArray[1];
    var temp = "";

    if (additionalURL) 
    {
        var tmpAnchor = additionalURL.split("#");
        var TheParams = tmpAnchor[0];
            TheAnchor = tmpAnchor[1];
        if(TheAnchor)
            additionalURL = TheParams;

        tempArray = additionalURL.split("&");

        for (var i=0; i<tempArray.length; i++)
        {
            if(tempArray[i].split('=')[0] != param)
            {
                newAdditionalURL += temp + tempArray[i];
                temp = "&";
            }
        }        
    }
    else
    {
        var tmpAnchor = baseURL.split("#");
        var TheParams = tmpAnchor[0];
            TheAnchor  = tmpAnchor[1];

        if(TheParams)
            baseURL = TheParams;
    }

    if(TheAnchor)
        paramVal += "#" + TheAnchor;

    var rows_txt = temp + "" + param + "=" + paramVal;
    return baseURL + "?" + newAdditionalURL + rows_txt;
}

////////////////////////////////////////////////////////////////////////////////
// math functions
////////////////////////////////////////////////////////////////////////////////

function calc_stddev(array) {
    if (!array || array.length === 0) {return 0;}
    const n = array.length
    const mean = array.reduce((a, b) => a + b) / n
    return Math.sqrt(array.map(x => Math.pow(x - mean, 2)).reduce((a, b) => a + b) / n)
}

function moving_average(xs,ys,window_size,weights, start_after_last_null=true, min_start_threshold=10) {

    // compute the helpers
    var last_notnull = 0;
    for (var i=0; i<ys.length; i++) {
        if (ys[i] !== null) {
            last_notnull = i;
        }
    }
    var last_null = 0;
    for (var i=0; i<last_notnull; i++) {
        if (ys[i] === null) {
            last_null = i;
        }
    }
    var min_start_threshold_i = null;
    for (var i=last_null; i<ys.length; i++) {
        if (min_start_threshold_i === null && weights[i] >= min_start_threshold) {
            min_start_threshold_i = i;
        }
    }

    var start_i = min_start_threshold_i;
    if (start_after_last_null && last_null > start_i) {
        start_i = last_null;
    }

    // compute the actual moving average
    var ys2 = [];
    var last_ys = null;
    var average = null;
    var total = null;
    for (var i=0; i<ys.length; i++) {
        if (ys[i] === null)
            next_ys = last_ys;
        else {
            next_ys = ys[i];
        }
        if (next_ys !== null && i >= start_i) {
            total = 0;
            average = 0;
            for (j=i; j>=0 && j>i-window_size; j--) {
                if (weights[j]>0 && ys[j] !== null) {
                    average += ys[j]*weights[j];
                    total += weights[j];
                }
            }
            average = average / total;
        }
        ys2.push(average);
        last_ys = next_ys;
    }
    for (i=last_notnull+1; i<ys2.length; i++) {
        ys2[i] = null;
    }
    return ys2;
}

////////////////////////////////////////////////////////////////////////////////
// JSON endpoint access/cache functions
////////////////////////////////////////////////////////////////////////////////

var json_cache = {};

function with_json(endpoint, params, callback) {
    if (!json_cache[endpoint]) {
        json_cache[endpoint] = {};
    }
    params_str = JSON.stringify(params, Object.keys(params).sort());
    if (!json_cache[endpoint][params_str]) {
        url = '/json/'+endpoint;
        for (let param in params) {
            url = updateURLParameter(url, param, params[param]);
        }
        console.log("url",url);
        const xhttp = new XMLHttpRequest();
        xhttp.onload = function() {
            try {
                parsed_json = JSON.parse(this.responseText);
            } catch(e) {
                alert('load_projection() failed for url='+url);
            }
            json_cache[endpoint][params_str] = parsed_json;
            json_cache[endpoint][params_str]['query'] = params['query'];
            callback(json_cache[endpoint][params_str]);
        }
        xhttp.open("GET", url);
        xhttp.send();
    }
    else {
        callback(json_cache[endpoint][params_str]);
    }
}

function form_to_dict(form_id) {
    elems = document.getElementById(form_id).elements;
    ret = {};
    for (let i=0; i<elems.length; i++) {
        if (elems[i].name) {
            ret[elems[i].name] = elems[i].value;
        }
    }
    return ret;
}

function serialize_form(form_id) {
    dict = form_to_dict(form_id);
    return JSON.stringify(dict, Object.keys(dict).sort());
}
