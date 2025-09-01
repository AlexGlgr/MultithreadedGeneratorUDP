import minimist from 'minimist';
import os from 'node:os';
import cluster from 'node:cluster';
import {env} from 'node:process';
import { execSync } from 'node:child_process';
import dgram  from 'dgram';
import net from 'node:net';

// Конфигурация по умолчанию
const DEFAULT_TCP_PORT = 9999;
const DEFAULT_TCP_IP = '10.120.100.51';


// Парсинг аргументов командной строки
const args = minimist(process.argv.slice(2), {
    alias: {
        p: 'port',
        i: 'ip',
    },
    default: {
        port: DEFAULT_TCP_PORT,
        ip: DEFAULT_TCP_IP
       
    }
});

const tcp_port = parseInt(args.port);
if (isNaN(tcp_port)) { console.error('Invalid TCP port, using default value', DEFAULT_TCP_PORT);
    tcp_port = DEFAULT_TCP_PORT;
}

const tcp_ip = args.ip;
if (net.isIP(tcp_ip) === 0) { console.error('Invalid IP address, using default value', DEFAULT_TCP_IP);
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

    const client = new net.Socket(); 

    const tcp_server = net.createServer((socket) => {
        socket.on('data', (data) => {
            const msg = JSON.parse(data.toString());
            switch (msg.com) {
                case 'register':
                    if (isNaN(parseInt(msg.port)) || net.isIP(msg.ip) === 0) {
                        console.error(`Cannot connect via recieved data! Port:${data.port}, IP: ${data.ip}`);
                        break;
                    }
                    try {
                        client.connect(msg.port, msg.ip, () => {
                            console.log('Connected to server ', msg.ip);

                            // Send data to the server
                            client.write(JSON.stringify({com: 'registered', name: 'cluster_server', status: 'running'}));
                        });
                    }
                    catch (e) {
                        console.error(e);
                    }
                    break;
                case 'packets':
                    gl_send_packets += msg.packets_send;
                    break;
                case 'drypackets':
                    const packets = msg.packets_send;
                    gl_send_packets += packets;
                    console.log(`Recieved ${packets} packets out of (${gl_send_packets} total)`);
                    break;
                case 'start':
                    gl_send_packets = 0;
                    gl_recieved_packets = 0;
                    gl_max_speed = 0;
                    speedData.forEach((record) => {
                        record.speed = 0;
                        record.pps = 0;
                        record.overall = 0;
                    });
                    start(msg.config);
                    try {
                        client.write(JSON.stringify({com: 'started', name: 'cluster_server', status: 'listenning'}));
                    }
                    catch (e) {
                        console.error(e);
                    }
                    break;
                case 'stop':
                    console.log(`Test is over. Max speed: ${gl_max_speed.toFixed(2)} Gbit/s | Send packets: ${gl_send_packets == 0 ? 'No data' : gl_send_packets} | Received packets: ${gl_recieved_packets} | `
                            + `Loss percentage: ${gl_send_packets == 0 ? 'No data' : `${((1 - (gl_recieved_packets/gl_send_packets))*100).toFixed(2)}%`}`);
                    workers.forEach((worker) => {
                        worker.kill();
                    });
                    clearInterval(interval);
                    try {
                        client.write(JSON.stringify({com: 'stopped', name: 'cluster_server', status: 'running'}));
                    }
                    catch (e) {
                        console.error(e);
                    }                    
                    break;
                case 'roundrobin':
                    cluster.schedulingPolicy = msg.rr === cluster.SCHED_RR ? cluster.SCHED_RR : cluster.SCHED_NONE;
                    console.log('Schedule policy set to ', cluster.schedulingPolicy);
                    try {
                        client.write(JSON.stringify({com: 'scheduled', name: 'cluster_server', status: 'running'}));
                    }
                    catch (e) {
                        console.error(e);
                    }
                    break;
                default:
                    break;
            }
            //gl_send_packets += JSON.parse(data.toString()).packets_send;
        });
    });
            
    tcp_server.listen(tcp_port, tcp_ip, () => {
        console.log('TCP connection on ', tcp_server.address());
    });

    if (os.type() == 'Linux') {
        const { pid } = process;
        execSync(`taskset -cp ${0} ${pid}`);
        console.log(`Main cluster with pid ${process.pid} running on Core ${0}`);
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
        process.send({port: env.port, speed: speed, pps: packetsReceived});

        packetsReceived = 0;
        bytesReceived = 0;
    }, 1000);
}

function start(jsonData) {
    let sensors = [];
    let group_count = jsonData.groups.length;
    for (let g = 0; g < group_count; g++) {
        let group = jsonData.groups[g].sensors;

        group.forEach((sensor) => {
            sensors.push({group: g, ip: sensor.dst.split(':')[0], port: parseInt(sensor.dst.split(':')[1])});            
        });
    }

    for (let i = 0; i < sensors.length; i++) {
        _env.port = sensors[i].port;
        _env.baseCPU = i;
        _env.ip = sensors[i].ip | '0.0.0.0';
        _env.mode = false;
        const worker = cluster.fork(_env);
        workers.push(worker);
        speedData.push({port: _env.port, speed: 0, pps: 0, overall: 0, grp: sensors[i].group});
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

            speedData.forEach((group) => {
                if (group.grp == group_idx) {
                    group_speed += group.speed;
                    group_packet_count += group.overall;
                    group_pps += group.pps; 
                }
            });

            if (group_idx == 0) console.log('\n\n');

            overall_speed += group_speed;
            overall_pps += group_pps;
            overall_packet_count += group_packet_count;

            console.log(`[GROUP:${group_idx}] Speed: ${group_speed.toFixed(2)} Gbit/s | Pps: ${group_pps} packets | `
                    + `Received: ${group_packet_count} packets`);

            if (gl_max_speed < overall_speed) gl_max_speed = overall_speed;
            gl_recieved_packets = overall_packet_count;

            console.log(`\nTotal Speed: ${overall_speed.toFixed(2)} Gbit/s | Pps: ${overall_pps} packets | Received: ${overall_packet_count} packets | `
                + `Avg. packet size: ${Math.floor(((overall_speed*1e9)/overall_pps)/8)} bytes`);            
        }      
    }, 2000);
}