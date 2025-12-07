#!/usr/bin/env node

import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';

const MCP_URL = process.env.MCP_URL || 'http://localhost:8080/mcp';
const PROMPT = process.argv[2] || 'show total sales for 2024';

console.log(`Connecting to MCP server at: ${MCP_URL}`);
console.log(`Sending prompt: "${PROMPT}"`);
console.log('');

const transport = new StreamableHTTPClientTransport({
  url: MCP_URL,
});

const client = new Client({
  name: 'test-client',
  version: '1.0.0',
}, {
  capabilities: {},
});

try {
  await client.connect(transport);
  console.log('✅ Connected to MCP server');
  console.log('');

  // Call the tools/call endpoint with the prompt
  const result = await client.callTool('handle_prompt', {
    prompt: PROMPT,
  });

  console.log('✅ Response received:');
  console.log('');
  
  if (result.content && result.content.length > 0) {
    const content = result.content[0];
    if (content.type === 'text') {
      console.log(content.text);
    } else {
      console.log(JSON.stringify(content, null, 2));
    }
  } else {
    console.log(JSON.stringify(result, null, 2));
  }

  await client.close();
  process.exit(0);
} catch (error) {
  console.error('❌ Error:', error.message);
  if (error.stack) {
    console.error(error.stack);
  }
  process.exit(1);
}



