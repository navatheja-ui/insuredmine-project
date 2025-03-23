const os = require('os');
const { exec } = require('child_process');
const fs = require('fs');
const path = require('path');

// Configuration
const CHECK_INTERVAL = 5000; // Check CPU every 5 seconds
const CPU_THRESHOLD = 70; // Restart server if CPU usage exceeds 70%
const SERVER_SCRIPT = 'index.js'; // Main server file to restart
const LOG_FILE = path.join(__dirname, 'server-restarts.log');

let serverProcess = null;

// Start the server initially
function startServer() {
  console.log('Starting server...');
  
  // Log the restart
  const timestamp = new Date().toISOString();
  fs.appendFileSync(LOG_FILE, `${timestamp} - Server started\n`);
  
  // Start the server as a child process
  serverProcess = exec(`node ${SERVER_SCRIPT}`, (error, stdout, stderr) => {
    if (error) {
      console.error(`Server error: ${error.message}`);
      return;
    }
    if (stderr) {
      console.error(`Server stderr: ${stderr}`);
      return;
    }
    console.log(`Server stdout: ${stdout}`);
  });
  
  // Log when server exits
  serverProcess.on('exit', (code, signal) => {
    console.log(`Server process exited with code ${code} and signal ${signal}`);
  });
  
  return serverProcess;
}

// Monitor CPU usage
function monitorCPU() {
  // Get CPU information
  const cpus = os.cpus();
  let totalIdle = 0;
  let totalTick = 0;
  
  // Calculate CPU usage across all cores
  cpus.forEach(cpu => {
    for (const type in cpu.times) {
      totalTick += cpu.times[type];
    }
    totalIdle += cpu.times.idle;
  });
  
  // Store current measurements for next comparison
  const idle = totalIdle / cpus.length;
  const total = totalTick / cpus.length;
  
  // Wait a second for next measurement
  setTimeout(() => {
    // Get new measurements
    const cpusNew = os.cpus();
    let totalIdleNew = 0;
    let totalTickNew = 0;
    
    // Calculate new values
    cpusNew.forEach(cpu => {
      for (const type in cpu.times) {
        totalTickNew += cpu.times[type];
      }
      totalIdleNew += cpu.times.idle;
    });
    
    const idleNew = totalIdleNew / cpus.length;
    const totalNew = totalTickNew / cpus.length;
    
    // Calculate the difference (this is the CPU usage)
    const idleDiff = idleNew - idle;
    const totalDiff = totalNew - total;
    const cpuUsage = 100 - Math.floor(100 * idleDiff / totalDiff);
    
    console.log(`Current CPU Usage: ${cpuUsage}%`);
    
    // Check if CPU usage exceeds threshold
    if (cpuUsage > CPU_THRESHOLD) {
      console.log(`CPU usage (${cpuUsage}%) exceeds threshold (${CPU_THRESHOLD}%). Restarting server...`);
      
      // Log the restart
      const timestamp = new Date().toISOString();
      fs.appendFileSync(LOG_FILE, `${timestamp} - Server restarted due to high CPU usage (${cpuUsage}%)\n`);
      
      // Kill the current server process
      if (serverProcess) {
        serverProcess.kill();
      }
      
      // Restart the server
      serverProcess = startServer();
    }
  }, 1000);
}

// Initial server start
serverProcess = startServer();

// Start CPU monitoring
console.log('Starting CPU monitoring...');
setInterval(monitorCPU, CHECK_INTERVAL);

console.log(`CPU monitor started. Threshold: ${CPU_THRESHOLD}%. Checking every ${CHECK_INTERVAL/1000} seconds.`);