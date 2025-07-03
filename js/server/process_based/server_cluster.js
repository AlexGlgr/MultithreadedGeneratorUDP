import minimist from 'minimist';
import os from 'node:os';
import cluster from 'node:cluster';
import {env} from 'node:process';
import { execSync } from 'node:child_process';
import dgram  from 'dgram';

// Конфигурация по умолчанию
const DEFAULT_PORT_BASE = 40000;
const DEFAULT_PACKET_SIZE = 8192; // 8KB

// Парсинг аргументов командной строки
const args = minimist(process.argv.slice(2), {
    alias: {
        s: 'sockets',
        p: 'portBase',
        z: 'packetSize',
        c: 'baseCPU'
    },
    default: {
        portBase: DEFAULT_PORT_BASE,
        packetSize: DEFAULT_PACKET_SIZE,
        baseCPU: 0,
        sockets: 1
    }
});

const sockets = parseInt(args.sockets);
const portBase = parseInt(args.portBase);
const packetSize = parseInt(args.packetSize);
const baseCPU = parseInt(args.baseCPU);

if (cluster.isPrimary) {
    const workers = [];
    let _env = Object.assign({}, process.env);
    for (let i = 0; i < sockets; i++) {
        _env.port = portBase;
        _env.packetSize = packetSize;
        _env.baseCPU = baseCPU + 1 + i;   
        const worker = cluster.fork(_env);
        workers.push(worker);
    }
    if (os.type() == 'Linux') {
        const { pid } = process;
        const cpu = baseCPU;
        execSync(`taskset -cp ${cpu} ${pid}`);
        console.log(`Process ${process.pid} running on Core ${cpu}`);
    }
} else {
    //console.log(`Worker info: ${env.port}, ${env.packetSize}, ${env.baseCPU}`);

    if (os.type() == 'Linux') {
        const { pid } = process;
        let cpu = env.baseCPU;
        execSync(`taskset -cp ${cpu} ${pid}`);
        console.log(`Process ${process.pid} running on Core ${cpu}`);
    }

    const socket = dgram.createSocket({
        type: 'udp4',
        reuseAddr: true,
        reusePort: true
    });
    let packetsReceived = 0;
    let pSize = env.packetSize;
    let overall = 0;
    
    socket.on('message', () => {   
        packetsReceived++;
    });
    
    socket.on('listening', () => {
        const address = socket.address();
        console.log(`Listening on ${address.address}:${address.port}`);
        socket.setRecvBufferSize(1024 * 1024 * 100);
    });
    
    socket.bind(env.port);

    let lastPrintTime = Date.now();
    setInterval(() => {
    const now = Date.now();
    const elapsed = (now - lastPrintTime) / 1000;
    const speed = (packetsReceived * pSize * 8) / (elapsed * 1e9);
    overall += packetsReceived;

    console.log(`[SERVER:${env.port}] Speed: ${speed.toFixed(2)} Gbit/s | `
            + `Received: ${overall.toLocaleString()} packets`);

    lastPrintTime = now;
    packetsReceived = 0;
    }, 1000);
}