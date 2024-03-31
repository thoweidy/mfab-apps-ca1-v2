const uuid = require('uuid');
const moment = require("moment");
const CryptoJS = require("crypto-js");
const axios = require('axios');
const fs = require('fs');
const { Client } = require('pg');
const crypto = require('crypto');

const getData = async (fromTime, toTime) => {

    // const fromTime = '2024-03-21T13:00:00'
    // const toTime = '2024-03-21T14:00:00'

    const pmquery = [
        { key: "from", value: fromTime },
        { key: "to", value: toTime },
        { key: "status", value: "Released,On Hold,Unreleased" },
        { key: "monitor_resources", value: "false" }
    ]
    const pmbody = {}


    /** Creating the Signature Base URL ********************************************************************/
    function createSignatureBaseURL() {
        let host = 'soasw.jpl.nasa.gov';
        let protocol = 'https';
        let path = 'JEMS/WipJobDelta_R1-2020-01-06/wipJobs';
        let baseURL = protocol + "://" + host + "/" + path;
        return baseURL;
    }

    /** Check if the request have Active Parameters, and return these parameters ***************************/
    function weHaveActiveRequestParameters() {
        let anyActiveParameters = false;

        if (pmquery.length > 0) {
            pmquery.forEach((p) => {
                if (p.disabled) {
                    // do nothing and ignore disabled parameters
                } else {
                    // if even one parameter is NOT disabled then we have active parameters to parse
                    anyActiveParameters = true;
                }
            });
        }
        return anyActiveParameters;
    }

    /** Parameters Normalizations **************************************************************************/
    function normalizeRequestParameters(parametersObject) {

        if (weHaveActiveRequestParameters()) {
            // We need to parse those and append them to the existing parameters Object
            pmquery.forEach((p) => {
                if (p.disabled) {
                    // do nothing and ignore disabled parameters
                } else {
                    parametersObject[p.key] = p.value;

                }
            });
        }


        let sortedParamKeys = Object.keys(parametersObject).sort();

        // Build up the query parameters in the sorted order and return it to the caller
        let sortedParameters = [];
        sortedParamKeys.forEach((k) => {
            let paramPair = k + "=" + parametersObject[k];
            sortedParameters.push(paramPair);
        });
        let normalizedParameters = sortedParameters.join('&');
        return normalizedParameters;
    }

    normalizeRequestParameters(pmquery)


    /** API Keys */
    const APPID = 'jplapi-5PSv8R59QeyNeDQJWaXYVGz9xWlVdLjLeuH6ugjp';
    const APIKey = 'df10162e689bc5741b51decda287d8da2c76eabd6c5f5c69ee4fc22e7e2f7d7e';
    const cycleID = uuid.v4()

    /** URL Varilables  */
    const NONCE = uuid.v4();

    const STR_EPOCH_TIME = moment(new Date().toUTCString()).valueOf();
    let startingParameters = {
        "atmosphere_app_id": APPID,
        "atmosphere_nonce": NONCE,
        "atmosphere_timestamp": STR_EPOCH_TIME,
        "atmosphere_signature_method": "HMACSHA256",
        "atmosphere_version": "1.0"
    };


    let baseSignatureURL = createSignatureBaseURL();
    let requestParameters = normalizeRequestParameters(startingParameters);
    var requestMethod = 'GET';
    let baseString = requestMethod + "&" + baseSignatureURL + "&" + requestParameters;
    var requestContentBase64String = "";


    if (pmbody) {
        var md5 = CryptoJS.MD5(pmbody.toString());
        requestContentBase64String = CryptoJS.enc.Base64.stringify(md5);
    }

    let utf8BaseString = CryptoJS.enc.Utf8.parse(baseString);
    let utf8APIKey = CryptoJS.enc.Utf8.parse(APIKey)
    let rawSignature = CryptoJS.HmacSHA256(utf8BaseString, utf8APIKey);
    let base64Signature = CryptoJS.enc.Base64.stringify(rawSignature);
    let finalSignature = encodeURIComponent(base64Signature);

    let auth_header = 'Atmosphere realm="http://communitymanager",' +
        'atmosphere_timestamp="' + STR_EPOCH_TIME + '",' +
        'atmosphere_nonce="' + NONCE + '"' + ',' +
        'atmosphere_app_id="' + APPID + '"' + ',' +
        'atmosphere_signature_method="HMACSHA256",' +
        'atmosphere_version="1.0",' +
        'atmosphere_signature="' + finalSignature + '"';

    let headers = {
        'Authorization': auth_header
    };

    const paramsURL = normalizeRequestParameters(pmquery)
    const parts = paramsURL.split('&from');
    const result = parts.length > 1 ? 'from' + parts[1] : '';

    // console.log("auth_header", auth_header)
    // console.log("-----------------------------------")



    let base = createSignatureBaseURL()
    let url = base + "?" + result
    // console.log("URL", url)
    // console.log("-----------------------------------")

    console.log("\n\x1b[30m\x1b[42mFetching Data from WIPJobsDelta API...\x1b[42m\x1b[0m\n");


    try {
        const response = await axios.get(url, { headers: headers })
        fs.writeFileSync('data.json', JSON.stringify(response.data))
        return response.data
    } catch (error) {
        console.error(error)
    }
}


module.exports = {
    getData
};