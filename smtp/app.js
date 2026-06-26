'use strict';

const { SMTPServer } = require('smtp-server');
const simpleParser = require('mailparser').simpleParser;
const nodemailer = require('nodemailer');
const WebSocket = require("ws");
const express = require("express");
const path = require('path');
const req = require('express/lib/request');
const res = require('express/lib/response');



const SMTP_SERVER_PORT = process.env.SMTP_SERVER_PORT  || 8025
const SERVER_PORT = process.env.SERVER_PORT || 8080
const WS_SERVER_PORT = process.env.WS_SERVER_PORT || 8081
const SERVER_HOST = process.env.SERVER_HOST || "localhost"
const WS_PROTOCOL = process.env.WS_PROTOCOL || "ws"
const WS_EX_PROTOCOL = process.env.WS_EX_PROTOCOL || "ws"
const WS_EX_SERVER_PORT = process.env.WS_EX_SERVER_PORT || 8081
const WS_EX_BASE_PATH = process.env.WS_EX_BASE_PATH || ""
const HTTP_PROTOCOL = process.env.HTTP_PROTOCOL || "http"
const INDEX = path.join(__dirname, "index.html"); // index address

const SMTP_FORWARD_HOST = process.env.SMTP_FORWARD_HOST || null;
const SMTP_FORWARD_PORT = parseInt(process.env.SMTP_FORWARD_PORT) || 8025;

const forwarder = SMTP_FORWARD_HOST ? nodemailer.createTransport({
    host: SMTP_FORWARD_HOST,
    port: SMTP_FORWARD_PORT,
    secure: false,
    auth: {
        user: process.env.SMTP_FORWARD_USER || 'relay',
        pass: process.env.SMTP_FORWARD_PASS || 'relay'
    },
    tls: { rejectUnauthorized: false }
}) : null;

if (forwarder) {
    console.log(`\x1b[33m SMTP Relay configured → ${SMTP_FORWARD_HOST}:${SMTP_FORWARD_PORT}\x1b[0m`);
}

const smtp_server = new SMTPServer({
    logger: false,

    banner: 'SMTP mock server, use UI to to check the actual message',

    disabledCommands: ['STARTTLS'],

    onAuth(auth, session, callback) {
        callback(null, { user: auth.username });
    },
    onData(stream, session, callback) {
        const chunks = [];
        stream.on('data', chunk => chunks.push(chunk));
        stream.on('end', () => {
            const raw = Buffer.concat(chunks);

            simpleParser(raw, {skipHtmlToText: false, skipImageLinks: false, skipTextToHtml: false, skipTextLinks: false, keepCidLinks: true})
                .then(parsed => {
                    parsed.type="MAIL";
                    console.log(`[SMTP] Mail received — from: ${parsed.from && parsed.from.text}, to: ${parsed.to && parsed.to.text}, subject: "${parsed.subject}", date: ${parsed.date}`);
                    console.log(`[SMTP] Active WS clients: ${socketServer.clients.size}`);
                    console.log(parsed);
                    broadCast(parsed);
                })
                .catch(err => {
                    console.log("Error: Unknown Error in the Socket Server");
                    console.log(err);
                });

            if (forwarder) {
                const from = session.envelope.mailFrom ? session.envelope.mailFrom.address : '';
                const to = session.envelope.rcptTo.map(r => r.address);
                forwarder.sendMail({ envelope: { from, to }, raw })
                    .then(() => console.log(`[RELAY] Forwarded to ${SMTP_FORWARD_HOST}:${SMTP_FORWARD_PORT}`))
                    .catch(err => console.log(`[RELAY] Forward failed: ${err.message}`));
            }

            console.log("[SMTP] Stream ended, sending callback");
            callback(null);
        });
    },
})

smtp_server.on('error', err => {
    console.log('Error: SMTP Error occurred ')
    console.log(err)
});

//Listen to the SMTP server
smtp_server.listen(SMTP_SERVER_PORT, SERVER_HOST)
console.log(`\x1b[33m SMTP Server Running on ${SERVER_HOST}:${SMTP_SERVER_PORT}\x1b[0m`)

const http_server = express();


//Set the route for index file
http_server.get('/', (req, res) => {
    res.sendFile(INDEX);
  });
  
//Set the route for configuration file
http_server.get('/config', (req, res) => {
    res.send({
        wsProtocol: WS_EX_PROTOCOL,
        wsPort: WS_EX_SERVER_PORT,
        basePath: WS_EX_BASE_PATH
     });
  });

//set the route for SMS
http_server.get('/sendsms', (req, res, next) => {
  try{
    let message ={}
    message.type = "SMS";
    message.date = new Date().toJSON();
    message.to = {"text": req.query.mobiles};
    message.from = {"text": req.query.sender};
    message.subject = "SMS: " + req.query.message;
    message.text = req.query.message;
    console.log(message);
    broadCast(message);
    res.sendStatus(200);
  }
  catch(error){
    console.log('Error: SMS Error occurred ');
    console.log(error);
  };
});

//Listen to the http port
  http_server.listen(SERVER_PORT, () => {
    console.log(`\x1b[33m HTTP Server Running on http://${SERVER_HOST}:${SERVER_PORT}\x1b[0m`);
  })

  /**
   * Listen on the Socker Server & Process messages
   * */

const socketServer = new WebSocket.Server({port: WS_SERVER_PORT,
    perMessageDeflate: {
      zlibDeflateOptions: {
        // See zlib defaults.
        chunkSize: 1024,
        memLevel: 7,
        level: 3
      },
      zlibInflateOptions: {
        chunkSize: 10 * 1024
      },
      // Other options settable:
      clientNoContextTakeover: true, // Defaults to negotiated value.
      serverNoContextTakeover: true, // Defaults to negotiated value.
      serverMaxWindowBits: 10, // Defaults to negotiated value.
      // Below options specified as default values.
      concurrencyLimit: 10, // Limits zlib concurrency for perf.
      threshold: 1024 // Size (in bytes) below which messages
      // should not be compressed if context takeover is disabled.
    } });
console.log(`\x1b[33m Socket Server Running on ws://${SERVER_HOST}:${WS_SERVER_PORT}\x1b[0m`);

function heartbeat() {
    this.isAlive = true;
  }

socketServer.on('connection', (socketClient, req) => {
    const clientIp = req.socket.remoteAddress;
    console.log(`[WS] Client connected — ip: ${clientIp}, total clients: ${socketServer.clients.size}`);
    socketClient.isAlive = true;
    socketClient.on('pong', heartbeat);

    socketClient.on('message', (message) => {
      console.log(`[WS] Message received from client: ${message}`);
      broadCast(message);
    });

    const interval = setInterval(function ping() {
        socketServer.clients.forEach(function each(ws) {
          if (ws.isAlive === false) {
            console.log('[WS] Client not responding to ping, terminating');
            return ws.terminate();
          }
          ws.isAlive = false;
          ws.ping();
        });
      }, 30000);

    socketClient.on('close', (code, reason) => {
        clearInterval(interval);
        console.log(`[WS] Client disconnected — code: ${code}, reason: ${reason}, remaining clients: ${socketServer.clients.size}`);
    });

    socketClient.on('error', (err) => {
        console.log(`[WS] Client error — ${err.message}`);
    });
  });


  /**
   * Broadcasts the mail to all the connected sockets
   * @param {*} message 
   */
  function broadCast(message){
    const payload = JSON.stringify(message);
    let sent = 0, skipped = 0;
    socketServer.clients.forEach((client) => {
        if (client.readyState === WebSocket.OPEN) {
          client.send(payload);
          sent++;
        } else {
          console.log(`[WS] Skipping client in state: ${client.readyState}`);
          skipped++;
        }
      });
    console.log(`[WS] broadCast — sent to ${sent} client(s), skipped ${skipped}`);
  }
