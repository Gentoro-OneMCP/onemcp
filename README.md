# Gentoro OneMCP

OneMCP is an open-source runtime that makes it easy for AI agents to use your API accurately and efficiently.

You provide your API materials — such as the specification, documentation, and authentication details (collectively called the handbook) — and OneMCP immediately exposes your system through a single, natural-language interface.

It removes the need to handcraft MCP tools or connectors while achieving high performance and low token cost through a smart execution-plan system designed for caching and reuse.

## Quick Start


First, install the CLI:

```bash
> curl -sSL https://raw.githubusercontent.com/Gentoro-OneMCP/onemcp/main/packages/cli/install.sh | bash
```

Then start chatting (auto-setup wizard launches on first run):

```bash
> onemcp chat
╔══════════════════════════════════════╗
║     Gentoro OneMCP - Chat Mode       ║
╚══════════════════════════════════════╝

Handbook: ecommerce-api
Provider: openai
MCP URL: http://localhost:8080/mcp
Type 'exit' to quit, 'clear' to clear history, 'switch' to change handbook
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Mock Server Active - Try These Example Queries:

  > Show me electronics sales in California last quarter.
  > List top customers by revenue.
  > Compare revenue trends by region.
  > What are the top-selling products this month?
  > Show me sales data for New York vs Texas.

Type "help" anytime for more commands.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

? You: Show sales data for New York vs Texas.
Response received
Agent:
Sales summary for 2024 (grouped by state)

- New York
  - Total sales: $12,340,000
  - Orders: 48,200
  - Average order value (AOV): $256

- Texas
  - Total sales: $9,870,000
  - Orders: 42,500
  - Average order value (AOV): $232

Key takeaways
- New York generated ~25% more total sales than Texas ($12.34M vs $9.87M).
- New York had about 13% more orders and a higher AOV (~$24 higher), suggesting both higher volume and larger order size in NY.

Report file: ~/handbooks/acme-analytics/logs/reports/execution-2025-11-25T16-49-08.335359Z.txt
```

## Running from Source

When working in the repository, you can run onemcp directly from the source code using the development script:

```bash
> packages/cli/scripts/onemcp chat
```

The script will automatically:
- Install npm dependencies if needed
- Build the CLI if needed
- Build the server JAR if needed
- Run the CLI with your command

All the same commands work as with the installed version:
- `packages/cli/scripts/onemcp chat` - Start chat mode
- `packages/cli/scripts/onemcp start` - Start the server
- `packages/cli/scripts/onemcp status` - Check server status
- `packages/cli/scripts/onemcp logs` - View server logs

**Requirements:**
- Node.js >= 20
- Java 21+ (for building the server)
- Maven (for building the server)

## Documentation

Full documentation is available at [https://onemcp.gentoro.com/docs](https://onemcp.gentoro.com/docs)

### CLI Details

For detailed CLI documentation, all commands, and advanced features, see the [CLI README](packages/cli/README.md).


## Contributing

We welcome contributions of all kinds — bug reports, feature requests, code improvements, documentation updates, and examples.

To contribute:

1. **Fork** the repository  
2. **Create a feature branch**  
   ```bash
   git checkout -b feature/my-improvement
   ```
3. **Make your changes**  
4. **Submit a Pull Request** with a clear explanation of the changes

Before opening a PR:

- Ensure tests pass  
- Follow existing code patterns  
- Avoid introducing breaking changes  
- Keep commits clean and focused

If you're new, check out issues labeled **good first issue** or **help wanted**.  
We’re happy to support new contributors!


## Join Our Community

Be part of the growing OneMCP ecosystem! Engage with other developers, share ideas, and stay updated on new features.

- **Slack:** Join the Gentoro Community at: [https://gentorocommunity.slack.com/signup#/domain-signup](https://gentorocommunity.slack.com/signup#/domain-signup)
- **GitHub Discussions:** Join conversations and ask questions in the Discussions tab  
- **Discord:** (Coming soon)  
- **Twitter/X:** Follow product updates and announcements  
- **Email:** Contact us at support@gentoro.com for questions or collaborations

We’d love to have you involved — whether you're building integrations, experimenting with agent workflows, or improving the core platform.

By contributing, you agree that your contributions will be licensed under the Apache 2.0 License.
