import { execSync, fork } from 'node:child_process';
import minimist from 'minimist';
import os from 'node:os';

// Конфигурация по умолчанию
const DEFAULT_PORT_BASE = 40000;
const DEFAULT_PACKET_SIZE = 8192; // 8KB

// Парсинг аргументов командной строки
const args = minimist(process.argv.slice(2), {
    alias: {
        p: 'portBase',
        z: 'packetSize'
    },
    default: {
        portBase: DEFAULT_PORT_BASE,
        packetSize: DEFAULT_PACKET_SIZE,
        baseCPU: 0,
        sockets: 1,
        baseIP: '0.0.0.0'
    }
});

const sockets = parseInt(args.sockets);
const portBase = parseInt(args.portBase);
const packetSize = parseInt(args.packetSize);
const baseCPU = parseInt(args.baseCPU);
const baseIP = args.baseIP;

const octetStrings = baseIP.split('.');
const octetIntegers = octetStrings.map(octet => parseInt(octet, 10));

if (isNaN(portBase)) throw new Error('Invalid port base');
if (isNaN(packetSize)) throw new Error('Invalid packet size');
if (isNaN(baseCPU)) throw new Error('Invalid core ID');

const processes = [];

for (let i = 0; i < sockets; i++) {
    setTimeout(() => {
        let sub_args = JSON.stringify({
                port: portBase,
                packet: packetSize,
                cpu: baseCPU + 1 + i,
                ip: octetIntegers.join('.')
            });
            octetIntegers[3]++;
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