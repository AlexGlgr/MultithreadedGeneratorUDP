const minimist = require('minimist');
import { fork } from 'node:child_process';

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
        sockets: 1
    }
});

const portBase = parseInt(args.portBase);
const packetSize = parseInt(args.packetSize);

if (isNaN(portBase)) throw new Error('Invalid port base');
if (isNaN(packetSize)) throw new Error('Invalid packet size');

 let sub_args = JSON.stringify({
        portBase,
        packetSize
    });

const child = fork('./server_reciever.js', [sub_args], {
        stdio: ['inherit', 'inherit', 'inherit', 'ipc']
});

process.on('SIGINT', () => {
    console.log('Stop signal sent to all child processes.');
    child.send({ type: 'SIGINT' });
    setTimeout(() => {
        child.kill('SIGTERM');
    }, 200);
});