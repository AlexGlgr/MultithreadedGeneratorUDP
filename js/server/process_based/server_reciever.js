import { argv } from 'process';
const dgram = require('dgram');

const STATS_INTERVAL = 3000;

const [ path, fn, args ] = argv;
let workerData = JSON.parse(args);

const socket = dgram.createSocket('udp4');
let packetsReceived = 0;
let socketCounter = 0;
let LastPacketIndex = 0;
let overall = 0;
let overallMissed = 0;

socket.on('message', msg => {
    if (msg.length === workerData.packetSize) {            
        packetsReceived++;
        LastPacketIndex = msg.readUint32BE(1) + 1;
        socketCounter++;
    }
});

socket.on('listening', () => {
    const address = socket.address();
    console.log(`Listening on ${address.address}:${address.port}`);
});

socket.bind(workerData.portBase);

let lastPrintTime = Date.now();
// Отправка статистики
setInterval(() => {
    const now = Date.now();
    const elapsed = (now - lastPrintTime) / 1000;
    const speed = (packetsReceived * workerData.packetSize * 8) / (elapsed * 1e9);

    overall += packetsReceived;
    overallMissed = (LastPacketIndex - socketCounter);

    console.log(`[SERVER] Speed: ${speed.toFixed(2)} Gbit/s | `
            + `Received: ${overall.toLocaleString()} | Lost: ${overallMissed.toLocaleString()} (${((overallMissed/overall)*100).toFixed(2)}%)| `
            + `Total: ${(overall+overallMissed).toLocaleString()}`);

    lastPrintTime = now;
    packetsReceived = 0;
    socketCounter = LastPacketIndex;
}, STATS_INTERVAL);