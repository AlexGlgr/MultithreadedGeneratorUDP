import { execSync, fork } from 'node:child_process';
import minimist from 'minimist';
import os from 'node:os';

// Конфигурация по умолчанию
const DEFAULT_PORT_BASE = 40000;
const DEFAULT_PACKET_SIZE = 8192; // 8KB
const DEFAULT_CORE = 0;
const DEFAULT_SOCKET_SIZE = 1;
const DEFAULT_IP = '0.0.0.0';


// Парсинг аргументов командной строки
const args = minimist(process.argv.slice(2), {
    alias: {
        p: 'portBase',
        z: 'packetSize',
        c: 'baseCPU',
        s: 'sockets',
        i: 'baseIP',
        m: 'mode',
        q: 'stock'
    },
    default: {
        portBase: DEFAULT_PORT_BASE,
        packetSize: DEFAULT_PACKET_SIZE,
        baseCPU: DEFAULT_CORE,
        sockets: DEFAULT_SOCKET_SIZE,
        baseIP: DEFAULT_IP
    }
});

const sockets = parseInt(args.sockets);
const portBase = parseInt(args.portBase);
const packetSize = parseInt(args.packetSize);
const baseCPU = parseInt(args.baseCPU);
const baseIP = args.baseIP;
const singularMode = (args.mode === undefined ? false : true);
const stockIP = (args.stock === undefined ? false : true);

const octetStrings = baseIP.split('.');
const octetIntegers = octetStrings.map(octet => parseInt(octet, 10));

if (isNaN(portBase)) throw new Error('Invalid port base');
if (isNaN(packetSize)) throw new Error('Invalid packet size');
if (isNaN(baseCPU)) throw new Error('Invalid core ID');

const processes = [];

for (let i = 0; i < sockets; i++) {
    setTimeout(() => {
        let sub_args = JSON.stringify({
                port: portBase + (singularMode ? 0 : i),
                packet: packetSize,
                cpu: baseCPU + i,
                ip: octetIntegers.join('.'),
                mode: singularMode
            });
            if (stockIP){
                octetIntegers[3]++;
            }
        const child = fork('./server_reciever.js', [sub_args], {
                stdio: ['inherit', 'inherit', 'inherit', 'ipc']
        });

        processes.push(child);
    },1000 * i);
}

// Привязка к CPU-ядру через taskset (Linux)
if (os.type() == 'Linux') {
    const { pid } = process;
    const cpu = baseCPU;
    execSync(`taskset -cp ${cpu} ${pid}`);
}

process.on('SIGINT', () => {
    console.log('Stop signal sent to all child processes.');
    processes.forEach(child => child.send({ type: 'SIGINT' }));
    setTimeout(() => {
        processes.forEach(child => child.kill('SIGTERM'));
    }, 200);
});