var mqtt = require('mqtt'); //https://www.npmjs.com/package/mqtt
var Topic = 'Test'; //subscribe to all topics
var Broker_URL = 'mqtt://192.168.0.115';
var Database_URL = '192.168.0.115';
// You have to change this address every time
const fs = require('fs');


var options = {
	clientId: 'MyMQTT',
	port: 1883,
	//username: 'mqtt_user',
	//password: 'mqtt_password',	
	keepalive : 60
};

var client  = mqtt.connect(Broker_URL, options);
client.on('connect', mqtt_connect);
client.on('reconnect', mqtt_reconnect);
client.on('error', mqtt_error);
client.on('message', mqtt_messsageReceived);
client.on('close', mqtt_close);


function mqtt_connect() {
    //console.log("Connecting MQTT");
    client.subscribe(Topic, mqtt_subscribe);
};

function mqtt_subscribe(err, granted) {
    console.log("Subscribed to " + Topic);
    if (err) {console.log(err);}
};

function mqtt_reconnect(err) {
    //console.log("Reconnect MQTT");
    //if (err) {console.log(err);}
	client  = mqtt.connect(Broker_URL, options);
};

function mqtt_error(err) {
    //console.log("Error!");
	//if (err) {console.log(err);}
};

function after_publish() {
	//do nothing
};

//receive a message from MQTT broker
function mqtt_messsageReceived(topic, message, packet) {
	var message_str = message.toString(); //convert byte array to string
	message_str = message_str.replace(/\n$/, ''); //remove new line
	//payload syntax: clientID,topic,message
	//if (countInstances(message_str) != 1) {
	//	console.log("Invalid payload");
	//	} else {	
		insert_message(topic, message_str, packet);
		console.log(message_str); 
		message_str = message_str + '\n';
		fs.appendFile('C:/Users/Admin/Downloads/ff414e868e80a6ee2fbc8261f8aebb8f-8234f772faf53e5d022dcc5f6169c7b2057549a3/MQTT_DATA.txt', message_str , err => {
			if (err) {
			console.error(err);
			}
  // file written successfully
		});
	//}
};

function mqtt_close() {
	//console.log("Close MQTT");
};

////////////////////////////////////////////////////
///////////////////// MYSQL ////////////////////////
////////////////////////////////////////////////////
var mysql = require('mysql'); //https://www.npmjs.com/package/mysql
//Create Connection
var connection = mysql.createConnection({
	host: Database_URL,
	port:4306,
	//user:'root',
	//password:'',
	user: "newuser",
	password: "mypassword",
	database: "mydb"
});

connection.connect(function(err) {
	if (err) throw err;
	console.log("Database Connected!");
});

//insert a row into the tbl_messages table
function insert_message(topic, message_str, packet) {
	var message_arr = extract_string(message_str); //split a string into an array
	var clientID= message_arr[0];
	var message = message_arr[1];
	var sql = "INSERT INTO ?? (??,??,??) VALUES (?,?,?)";
	var params = ['tbl_messages', 'clientID', 'topic', 'message', clientID, topic, message];
	sql = mysql.format(sql, params);	
	console.log(sql);
	connection.query(sql, function (error, results) {
		if (error) throw error;
		console.log("Message added: " + message_str);
	}); 
};	

//split a string into an array of substrings
function extract_string(message_str) {
	var message_arr = message_str.split(","); //convert to array	
	return message_arr;
};	

//count number of delimiters in a string
var delimiter = ",";
function countInstances(message_str) {
	var substrings = message_str.split(delimiter);
	return substrings.length - 1;
};
