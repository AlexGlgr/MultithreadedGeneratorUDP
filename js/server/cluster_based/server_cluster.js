import minimist from 'minimist';
import os from 'node:os';
import cluster from 'node:cluster';
import { Server } from 'socket.io';
import { createServer } from 'node:http';
import dgram from 'dgram';
import net from 'node:net';
import { env } from 'node:process';
import { execSync } from 'node:child_process';
import { createLimitedWriteStream } from './writeStats.mjs';

// Конфигурация по умолчанию
const DEFAULT_TCP_PORT = 9999;
const DEFAULT_TCP_IP = '10.120.1.222';

const MAX_CORES = getMaxCores();

// Парсинг аргументов командной строки
const args = minimist(process.argv.slice(2), {
    string: ['port', 'ip', 'logFile'],
    number: ['maxLog'],
    boolean: 'def',
    alias: {
        p: 'port',
        i: 'ip',
        d: 'def',
        l: 'logFile',
        m: 'maxLog'
    },
    default: {
        port: DEFAULT_TCP_PORT,
        ip: DEFAULT_TCP_IP,
        def: false,
        logFile: null,
        maxLog: 1024*1024
    }
});

const def = args.def & 1;

const tcp_port = parseInt(args.port);
const tcp_ip = args.ip;
const logPath = args.logFile;
const maxLog = args.maxLog;

if (isNaN(tcp_port)) {
    console.error('Invalid TCP port, using default value', DEFAULT_TCP_PORT);
    tcp_port = DEFAULT_TCP_PORT;
}

if (net.isIP(tcp_ip) === 0) {
    console.error('Invalid IP address, using default value', DEFAULT_TCP_IP);
    tcp_ip = DEFAULT_TCP_IP;
}

let workers = [];
const speedData = [];
let _env;
let interval;
let gl_recieved_packets = 0;
let gl_send_packets = 0;
let gl_max_speed = 0;

if (cluster.isPrimary) {
    _env = Object.assign({}, process.env);
    // const client = new net.Socket();
    if (def) {
        console.log('Default Start');
        gl_send_packets = 0;
        gl_recieved_packets = 0;
        gl_max_speed = 0;
        speedData.forEach((record) => {
            record.speed = 0;
            record.pps = 0;
            record.overall = 0;
        });
        start({ def: true });
    } else {
        const httpServer = createServer();
        const io = new Server(httpServer, {
            cors: {
                origin: "*",
                methods: ["GET", "POST"],
            },
            pingTimeout: 60000,
            pingInterval: 10000,
            connectionStateRecovery: {
                maxDisconnectionDuration: 2 * 60 * 1000,
            }
        });

        io.on('connection', (socket) => {
            console.log('Client connected:', socket.id);

            // Обработка команд от клиента
            socket.on('register-req', command => handleRegister(socket, command));
            socket.on('packets', command => handlePackets(command));
            socket.on('drypackets', command => handleDryPackets(command));
            socket.on('start-req', command => handleStart(socket, command));
            socket.on('stop-req', command => handleStop(socket, () => {
                socket.removeAllListeners('packets');
                socket.removeAllListeners('drypackets');
            }));
            socket.on('roundrobin-req', command => handleRoundRobin(socket, command));

            socket.on('disconnect', (reason) => {
                console.log('Client disconnected:', socket.id, reason);
            });

            socket.on('error', (error) => {
                console.error('Socket error:', socket.id, error);
            });
        });

        /**
         * Обработка регистрации
         */
        function handleRegister(socket, command) {
            console.log('Connected to server', command);

            socket.emit('register-res', {
                com: 'registered',
                name: 'cluster_server',
                status: 'running'
            });
        }

        /**
         * Обработка пакетов
         */
        function handlePackets(command) {
            gl_send_packets = command.packets_send;
            console.log(command);
        }

        /**
         * Обработка drypackets
         */
        function handleDryPackets(command) {
            const packets = command.packets_send;
            gl_send_packets += packets;
            console.log(`Received ${packets} packets out of (${gl_send_packets} total)`);
        }

        /**
         * Обработка старта
         */
        function handleStart(socket, command) {
            console.log('Start command received');

            gl_send_packets = 0;
            gl_recieved_packets = 0;
            gl_max_speed = 0;

            // Сброс статистики
            speedData.forEach((record) => {
                record.speed = 0;
                record.pps = 0;
                record.overall = 0;
            });

            // Запуск основного процесса
            start(command.config);

            socket.emit('start-res', {
                com: 'started',
                name: 'cluster_server',
                status: 'listening'
            });
        }

        /**
         * Обработка остановки
         */
        function handleStop(socket, cb) {
            console.log('Stop command received');

            const lossPercentage = gl_send_packets === 0 ?
                'No data' :
                `${((1 - (gl_recieved_packets / gl_send_packets)) * 100).toFixed(2)}%`;

            console.log(`Test is over. Max speed: ${gl_max_speed.toFixed(2)} Gbit/s | ` +
                `Send packets: ${gl_send_packets === 0 ? 'No data' : gl_send_packets} | ` +
                `Received packets: ${gl_recieved_packets} | ` +
                `Loss percentage: ${lossPercentage}`);

            // Остановка воркеров
            workers.forEach((worker) => {
                worker.kill();
            });
            workers = [];

            // Очистка интервалов (если есть)
            if (interval) {
                clearInterval(interval);
            }

            socket.emit('stop-res', {
                com: 'stopped',
                name: 'cluster_server',
                status: 'running'
            });
            cb();
        }

        /**
         * Обработка roundrobin
         */
        function handleRoundRobin(socket, command) {
            cluster.schedulingPolicy = command.rr === 1 ?
                cluster.SCHED_RR :
                cluster.SCHED_NONE;

            console.log('Schedule policy set to', cluster.schedulingPolicy);

            socket.emit('roundrobin-res', {
                com: 'scheduled',
                name: 'cluster_server',
                status: 'running'
            });
        }

        // Запуск сервера
        httpServer.listen(tcp_port, tcp_ip, () => {
            console.log(`Server running on http://${tcp_ip}:${tcp_port}`);
        });
    }
} else {
    if (os.type() == 'Linux') {
        const { pid } = process;
        let cpu = env.baseCPU;
        execSync(`taskset -cp ${cpu} ${pid}`);
    }

    const socket = dgram.createSocket({
        type: 'udp4',
        reuseAddr: env.mode,
        reusePort: env.mode
    });
    let bytesReceived = 0;
    let packetsReceived = 0;

    socket.on('message', (msg) => {
        bytesReceived += msg.length;
        packetsReceived++;
    });

    socket.on('listening', () => {
        const address = socket.address();
        socket.setRecvBufferSize(1024 * 1024 * 100);
    });

    socket.bind(env.port, env.ip);

    setInterval(() => {
        const speed = (bytesReceived * 8) / 1e9;
        process.send({ port: env.port, speed: speed, pps: packetsReceived });

        packetsReceived = 0;
        bytesReceived = 0;
    }, 1000);
}

function getMaxCores() {
    return execSync('nproc --all');
        //return execSync(`grep "cpu cores" /proc/cpuinfo | sort -u | cut -d ":" -f 2 | awk '{s+=$1} END {print s}'`)
}

function start(jsonData) {
    console.log(jsonData);
    let stat_n = Object.fromEntries(jsonData.groups.map(g => [g.name, 0]));
    if (logPath) {
        write = createLimitedWriteStream(logPath, maxLog*1024);
        write(`stat_n;group_name;pps;total\n`);
    }
    
    let sensors = [];
    let group_count;
    if (def) {
        group_count = MAX_CORES;
        for (let i = 0; i < group_count; i++) {
            console.log(`New socket: ${40000 + i}`);
            sensors.push({ group: i, ip: '0.0.0.0', port: 40000 + i });
        }
    }
    else {
        group_count = jsonData.groups.length;
        for (let g = 0; g < group_count; g++) {
            let group = jsonData.groups[g].sensors;

            group.forEach((sensor) => {
                sensors.push({ group: g, ip: sensor.dst.split(':')[0], port: parseInt(sensor.dst.split(':')[1]) });
            });
        }
    }


    for (let i = 0; i < sensors.length; i++) {
        _env.port = sensors[i].port;
        _env.baseCPU = i % MAX_CORES;
        //_env.baseCPU = MAX_CORES - 1 - (i % (MAX_CORES - 1));
        _env.ip = sensors[i].ip | '0.0.0.0';
        _env.mode = false;
        // break;
        const worker = cluster.fork(_env);
        workers.push(worker);
        speedData.push({ port: _env.port, speed: 0, pps: 0, overall: 0, grp: sensors[i].group });
        worker.on('message', (msg) => {
            speedData[i].speed = msg.speed;
            speedData[i].pps = msg.pps;
            speedData[i].overall += msg.pps;
        });
    }

    interval = setInterval(() => {
        let overall_speed = 0;
        let overall_packet_count = 0;
        let overall_pps = 0;


        for (let group_idx = 0; group_idx < group_count; group_idx++) {
            let group_speed = 0;
            let group_packet_count = 0;
            let group_pps = 0;
            let group_name;

            speedData.forEach((group) => {
                if (group.grp == group_idx) {
                    group_speed += group.speed;
                    group_packet_count += group.overall;
                    group_pps += group.pps;
                    group_name = group.name;
                }
            });

            if (group_idx == 0) console.log('\n\n');

            overall_speed += group_speed;
            overall_pps += group_pps;
            overall_packet_count += group_packet_count;

            console.log(`[GROUP:${group_idx}] Speed: ${group_speed.toFixed(2)} Gbit/s | Pps: ${group_pps} packets | `
                + `Received: ${group_packet_count} packets`);
            
            write(`${stat_n[group_idx]};${group_name};${group_pps};${tx_sent}\n`)

            if (gl_max_speed < overall_speed) gl_max_speed = overall_speed;
            gl_recieved_packets = overall_packet_count;
        }
        console.log(`\nTotal Speed: ${overall_speed.toFixed(2)} Gbit/s | Pps: ${overall_pps} packets | Received: ${overall_packet_count} packets | `
            + `Avg. packet size: ${Math.floor(((overall_speed * 1e9) / overall_pps) / 8)} bytes`);
    }, 2000);
}