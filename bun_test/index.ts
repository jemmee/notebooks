// curl -fsSL https://bun.sh/install | bash
//
// mkdir bun_test && cd bun_test
// bun init -y
// bun add @bun/sql
//
// bun index.ts
//
// curl http://localhost:3000/api/status
//
// curl -X POST http://localhost:3000/api/log -H "Content-Type: application/json" -d '{"service": "storage-firewall", "status": "OPTIMIZING"}'
//
// bun build ./index.ts --compile --outfile server-app
//
// ./server-app

import { Database } from "bun:sqlite";
import { SQL } from "bun"; // ◄— Import native SQL here

// 1. Initialize your in-memory SQLite database (remains unchanged)
const sqlite = new Database(":memory:");
sqlite.run(`
  CREATE TABLE IF NOT EXISTS service_status (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    service TEXT,
    status TEXT,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
  )
`);

const insertLog = sqlite.prepare(
    "INSERT INTO service_status (service, status) VALUES ($service, $status)"
);
const getLogs = sqlite.prepare("SELECT * FROM service_status ORDER BY id DESC LIMIT 5");

// Seed initial data
insertLog.run({ $service: "auth-gateway", $status: "HEALTHY" });
insertLog.run({ $service: "payment-api", $status: "DEGRADED" });

// 2. Updated Postgres Client Initialization
// Bun.SQL automatically parses the connection string and detects the Postgres protocol
const pgConfigured = !!process.env.PG_URL;
const sql = pgConfigured ? new SQL(process.env.PG_URL!) : null; // ◄— Use 'new SQL()'

// 3. Launch Bun.serve() (remains identical...)
const server = Bun.serve({
    port: 3000,
    async fetch(request) {
        const url = new URL(request.url);

        if (request.method === "GET" && url.pathname === "/api/status") {
            const logs = getLogs.all();
            return Response.json({
                engine: "Bun v" + Bun.version,
                runtime: "JavaScriptCore",
                database: "SQLite (In-Memory)",
                recent_logs: logs,
            });
        }

        if (request.method === "POST" && url.pathname === "/api/log") {
            try {
                const body = await request.json();
                if (!body.service || !body.status) {
                    return new Response("Missing service or status fields", { status: 400 });
                }

                insertLog.run({ $service: body.service, $status: body.status });

                // Corrected parameterized template literal syntax for native Bun SQL
                if (sql) {
                    await sql`INSERT INTO external_logs (service, status) VALUES (${body.service}, ${body.status})`;
                }

                return Response.json({ success: true, message: "Log registered successfully" });
            } catch (err) {
                return new Response("Invalid JSON Payload", { status: 400 });
            }
        }

        return new Response("Endpoint not found", { status: 404 });
    },
});

console.log(`🚀 Bun server actively running at http://localhost:${server.port}`);