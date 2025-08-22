import minimist from 'minimist';
import os from 'node:os';
import cluster from 'node:cluster';
import {env} from 'node:process';
import { execSync } from 'node:child_process';
import dgram  from 'dgram';
import net from 'node:net';
import { readFile } from 'node:fs';

// Конфигурация по умолчанию
const DEFAULT_PORT_BASE = 40000;
const DEFAULT_CORE = 0;
const DEFAULT_SOCKET_SIZE = 1;
const DEFAULT_IP = '0.0.0.0';

// Парсинг аргументов командной строки
const args = minimist(process.argv.slice(2), {
    alias: {
        p: 'portBase',
        o: 'portCount',
        r: 'roundRobin',
        c: 'baseCPU',
        s: 'sockets',
        i: 'baseIP',
        m: 'mode',
        q: 'stock',
        f: 'file'
    },
    default: {
        portBase: DEFAULT_PORT_BASE,
        portCount: 1,
        baseCPU: DEFAULT_CORE,
        sockets: DEFAULT_SOCKET_SIZE,
        baseIP: DEFAULT_IP,
        roundRobin: 0,
        file: './test.json'
    }
});

const sockets = parseInt(args.sockets);
const portBase = parseInt(args.portBase);
const portCount = parseInt(args.portCount);
const baseCPU = parseInt(args.baseCPU);
const baseIP = args.baseIP;
const singularMode = (args.mode === undefined ? false : true);
const stockIP = (args.stock === undefined ? false : true);
const roundRobin = parseInt(args.roundRobin) === 0 ? 0 : 1;
const file = args.file;

const octetStrings = baseIP.split('.');
const octetIntegers = octetStrings.map(octet => parseInt(octet, 10));

if (isNaN(portBase)) throw new Error('Invalid port base');
if (isNaN(baseCPU)) throw new Error('Invalid core ID');

if (cluster.isPrimary) {
    cluster.schedulingPolicy = roundRobin === 0 ? cluster.SCHED_NONE : cluster.SCHED_RR;
    let gl_recieved_packets = 0;
    let gl_send_packets = 0;
    let gl_max_speed = 0;
    let countDown = 0;
    const workers = [];
    const speedData = [];
    let _env = Object.assign({}, process.env);

    readFile(file, 'utf8', (err, data) => {
        if (err) {
            console.error(err);
        }
        else {
            const jsonData = JSON.parse(data);
            console.log(jsonData);
        }
    });

    for (let i = 0; i < sockets*portCount; i++) {
        _env.port = Math.floor(portBase + (i / sockets));
        _env.baseCPU = baseCPU + i;
        _env.ip = octetIntegers.join('.');
        _env.mode = singularMode;
        const worker = cluster.fork(_env);
        workers.push(worker);
        if (stockIP){
            octetIntegers[3]++;
        }
        speedData.push({port: _env.port, speed: 0, pps: 0, overall: 0});
        worker.on('message', (msg) => {
            speedData[i].speed = msg.speed;
            speedData[i].pps = msg.pps;
            speedData[i].overall += msg.pps;
        });
    }
    if (os.type() == 'Linux') {
        const { pid } = process;
        const cpu = baseCPU;
        execSync(`taskset -cp ${cpu} ${pid}`);
        console.log(`Main cluster with pid ${process.pid} running on Core ${cpu}`);
    }

    const tcp_server = net.createServer((socket) => {
        socket.on('data', (data) => {
            gl_send_packets += JSON.parse(data.toString()).packets_send;
        });
    });
    
    tcp_server.listen(9999, () => {
        console.log('TCP connection on ', tcp_server.address());
    });

    setInterval(() => {
        let overall_speed = 0;
        let overall_packet_count = 0;
        let overall_pps = 0;
        
        
        for (let group_idx = 0; group_idx < portCount; group_idx++) {
            let group_speed = 0;
            let group_packet_count = 0;
            let group_port = 0;
            let group_pps = 0;

            for (let cluster_idx = 0; cluster_idx < sockets; cluster_idx++) {

                let spData_key = (sockets*group_idx)+cluster_idx;

                group_speed += speedData[spData_key].speed;
                group_packet_count += speedData[spData_key].overall;
                group_port = speedData[spData_key].port;
                group_pps += speedData[spData_key].pps;               
            }
            if (group_speed > 0) {
                if (group_idx == 0) console.log('\n\n');

                overall_speed += group_speed;
                overall_pps += group_pps;
                overall_packet_count += group_packet_count;

                console.log(`[SERVER PORT:${group_port}] Speed: ${group_speed.toFixed(2)} Gbit/s | Pps: ${group_pps} packets | `
                        + `Received: ${group_packet_count} packets`);
            }
            
        }
        if (overall_speed > 0) {
            countDown = 6;
            if (gl_max_speed < overall_speed) gl_max_speed = overall_speed;
            console.log('\n');
            console.log(`Total Speed: ${overall_speed.toFixed(2)} Gbit/s | Pps: ${overall_pps} packets | Received: ${overall_packet_count} packets | `
                + `Avg. packet size: ${Math.floor(((overall_speed*1e9)/overall_pps)/8)} bytes`);
            gl_recieved_packets = overall_packet_count;
        }
        else if (countDown > 0) countDown--;

        if (countDown == 2) {
            console.log(`Test is over. Max speed: ${gl_max_speed.toFixed(2)} Gbit/s | Send packets: ${gl_send_packets == 0 ? 'No data' : gl_send_packets} | Received packets: ${gl_recieved_packets} | `
                + `Loss percentage: ${gl_send_packets == 0 ? 'No data' : `${((1 - (gl_recieved_packets/gl_send_packets))*100).toFixed(2)}%`}`);
            gl_send_packets = 0;
            gl_recieved_packets = 0;
            gl_max_speed = 0;
            speedData.forEach((data) => {
                data.speed = 0;
                data.pps = 0;
                data.overall = 0;
            })
        }        
    }, 2000);

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