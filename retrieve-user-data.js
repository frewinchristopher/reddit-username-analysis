// requires
const axios = require('axios');
const { Client } = require('pg');

// program constants
const sRootURL = "https://reddit.com/user/";
const sEndURL = "/about.json";

// connect to reddit usernames postgres 
const client = new Client({
  user: process.env.REDDIT_DATA_DB_USER,
  host: process.env.REDDIT_DATA_DB_HOST,
  database: process.env.REDDIT_DATA_DB,
  password: process.env.REDDIT_DATA_DB_PASSWORD,
  port: process.env.REDDIT_DATA_DB_PORT,
});
client.connect();

function rateLimit(fn, delay, context) {
    var canInvoke = true,
        queue = [],
        timeout,
        limited = function () {
            queue.push({
                context: context || this,
                arguments: Array.prototype.slice.call(arguments)
            });
            if (canInvoke) {
                canInvoke = false;
                timeEnd();
            }
        };
    function run(context, args) {
        fn.apply(context, args);
    }
    function timeEnd() {
        var e;
        if (queue.length) {
            e = queue.splice(0, 1)[0];
            run(e.context, e.arguments);
            timeout = window.setTimeout(timeEnd, delay);
        } else
            canInvoke = true;
    }
    limited.reset = function () {
        window.clearTimeout(timeout);
        queue = [];
        canInvoke = true;
    };
    return limited;
}

let sQuery = { text: 'SELECT * FROM user_about;', rowMode: 'array' };

 client.query(sQuery, (err, res) => {
   if (err) {
     console.log(err.stack)
   } else { // now check if json is already found for this user
     res.rows.forEach((oRow, iIndex) => {
         if (oRow[1] === null) { // only retreive if about_response is 'null'
           setTimeout(() => retrieveData(oRow), 20*iIndex);
         } else {
           console.log("Already retrieved json for user " + oRow[0]);
         }
     });
   }
 });
 
function retrieveData(oRow) {
  axios.get(sRootURL + oRow[0] + sEndURL)
       .then(oResponse => saveJSONToDB(oResponse.data, oRow))
       .catch(oError => {
         saveJSONToDB(oError.response.data, oRow)
       });
}

function saveJSONToDB(oData, oRow) {
  console.log("here in save function!");
  console.log(oData);
    let sQuery = {
  		text: "UPDATE user_about SET about_response = '" + JSON.stringify(oData) + "' WHERE username = '" + oRow[0] + "';",
  	};
   client.query(sQuery, (err, res) => {
     if (err) {
       console.log(err.stack)
     } else {
       console.log("Commited record: User: " + oRow[0] + " data:" + JSON.stringify(oData));
     }
   });
}


