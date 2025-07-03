import { argv } from 'process';
import { execSync } from 'child_process';
import dgram  from 'dgram';
import os from 'os';

const STATS_INTERVAL = 1000;

const [ path, fn, args ] = argv;
let workerData = JSON.parse(args);

const socket = dgram.createSocket({
    type: 'udp4',
    reuseAddr: true,
    reusePort: true
});
let packetsReceived = 0;
let pSize = workerData.packetSize;
let overall = 0;

socket.on('message', () => {   
    packetsReceived++;
});

socket.on('listening', () => {
    const address = socket.address();
    console.log(`Listening on ${address.address}:${address.port}`);
    socket.setRecvBufferSize(1024 * 1024 * 100);
});

socket.bind(workerData.portBase);
//socket.setSendBufferSize(1024 * 1024 * 100);


let lastPrintTime = Date.now();
// Отправка статистики
setInterval(() => {
    const now = Date.now();
    const elapsed = (now - lastPrintTime) / 1000;
    const speed = (packetsReceived * pSize * 8) / (elapsed * 1e9);
    overall += packetsReceived;

    console.log(`[SERVER:${workerData.portBase}] Speed: ${speed.toFixed(2)} Gbit/s | `
            + `Received: ${overall.toLocaleString()} packets`);

    lastPrintTime = now;
    packetsReceived = 0;
}, STATS_INTERVAL);

// Привязка к CPU-ядру через taskset (Linux)
if (os.type() == 'Linux') {
    const { pid } = process;
    let i = workerData.threadIndex;
    const cpu = (i + 1) < os.cpus().length ? i : i - os.cpus().length;
    execSync(`taskset -cp ${cpu} ${pid}`);
    console.log(`Process ${process.pid} running on Core ${cpu}`);
}