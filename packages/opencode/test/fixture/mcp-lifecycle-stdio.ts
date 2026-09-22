if (process.argv.includes("--hang")) {
  const pidFile = process.env.MCP_LIFECYCLE_PID_FILE
  if (!pidFile) throw new Error("MCP_LIFECYCLE_PID_FILE is required")
  await Bun.write(pidFile, String(process.pid))
  // A pending promise alone does not keep the child alive while initialization hangs.
  process.stdin.resume()
  await new Promise(() => {})
}

const { Server } = await import("@modelcontextprotocol/sdk/server/index.js")
const { StdioServerTransport } = await import("@modelcontextprotocol/sdk/server/stdio.js")
const { ListToolsRequestSchema } = await import("@modelcontextprotocol/sdk/types.js")

const server = new Server({ name: "mcp-lifecycle-stdio", version: "1.0.0" }, { capabilities: { tools: {} } })

server.setRequestHandler(ListToolsRequestSchema, () =>
  Promise.resolve({
    tools: [
      {
        name: "current_directory",
        description: process.cwd(),
        inputSchema: { type: "object", properties: {} },
      },
    ],
  }),
)

await server.connect(new StdioServerTransport())
