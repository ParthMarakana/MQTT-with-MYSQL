var address = require ('address');
var ip = address.ip()
var mqtt = require('mqtt'); 
var Topic = 'Test'; 
var Broker_URL = 'mqtt://' + ip;
var Database_URL = ip;


// {"DN":1001,"DID":"Parth","DS":1,"SSR":0,"RPM":666,"PSI":60,"CT":"20220922154042","PV":"1.0","HV":"1.2","FV":"1.10"}

var options = {
	clientId: 'MyMQTT',
	port: 1883,
	//username: '',
	//password: '',	
	connectTimeout:1000, 
	debug:true,
	//keepalive : 60
};

var client  = mqtt.connect(Broker_URL, options);
client.on('connect', mqtt_connect);
client.on('reconnect', mqtt_reconnect);
client.on('error', mqtt_error);
client.on('message', mqtt_messsageReceived);
client.on('close', mqtt_close);


function mqtt_connect() {
    console.log("Connecting MQTT");
    client.subscribe(Topic, mqtt_subscribe);
};

 function mqtt_reconnect(err) {
	//client.end()
    console.log("Reconnect MQTT");
   if (err) {console.log(err);}		
/* 	try{
		console.log("i am here");
	client  = await mqtt.connect(Broker_URL, options);
	} catch (err){
		console.log("i am there");
		console.log(err)
	} */
	//client.on('connect', mqtt_connect);

};


function mqtt_subscribe(err, granted) {
    console.log("Subscribed to " + Topic);
    //if (err) {console.log(err);}
};


function mqtt_error(err) {
	
    //console.log("Error!");
	//if (err) {console.log(err);}
};



//receive a message from MQTT broker
function mqtt_messsageReceived(topic, message, packet) {
	var message_str = message.toString(); //convert byte array to string
	//message_str = message_str.replace(/\n$/, ''); //remove new line
	

		insert_message(topic, message_str, packet);
	
	
};

function mqtt_close() {
	console.log("Close MQTT");
};

var mysql = require('mysql');
//Create Connection
var connection = mysql.createConnection({
	host: Database_URL,
	port : 4306,
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
	Json_Data = JSON.parse(message_str);
	var sql = "INSERT INTO ?? (??,??,??,??,??,??,??,??,??,??) VALUES (?,?,?,?,?,?,?,?,?,?)";
	var params = ['tbl_messages', 'DN', 'DID', 'DS','SSR','RPM','PSI','CT','PV','HV','FV', Json_Data.DN, Json_Data.DID, Json_Data.DS, Json_Data.SSR,Json_Data.RPM, Json_Data.PSI,Json_Data.CT, Json_Data.PV ,Json_Data.HV,Json_Data.FV];
	sql = mysql.format(sql, params);	
	
	connection.query(sql, function (error, results) {
		if (error) throw error;
		console.log("Message added: " + message_str);
	}); 
};	