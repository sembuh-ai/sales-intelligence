#!/usr/bin/env python3
"""CLI tool — query Slack via MCP + Claude tool_use."""

import asyncio
import os
import sys
from contextlib import AsyncExitStack

from anthropic import Anthropic
from dotenv import load_dotenv
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

load_dotenv()

CLAUDE_MODEL = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-5-20250929")
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID", "")

SYSTEM_PROMPT = (
    "You are a Slack assistant. Use the available Slack tools to answer user questions. "
    "Be concise. Use tables/lists where helpful. Answer in the same language as the user."
    + (
        f"\n\nIMPORTANT: Default channel ID is {SLACK_CHANNEL_ID}. "
        "When user asks to read or post messages without specifying a channel, use this channel_id. "
        "Always use this channel unless user explicitly names a different one."
        if SLACK_CHANNEL_ID
        else ""
    )
)


class SlackMCPClient:
    def __init__(self):
        self.exit_stack = AsyncExitStack()
        self.session: ClientSession | None = None
        self.anthropic = Anthropic()
        self.tools = []

    async def connect(self):
        """Spawn Slack MCP server and connect via stdio."""
        server_params = StdioServerParameters(
            command="npx",
            args=["-y", "@modelcontextprotocol/server-slack"],
            env={
                **os.environ,
                "SLACK_BOT_TOKEN": os.getenv("SLACK_BOT_TOKEN", ""),
                "SLACK_TEAM_ID": os.getenv("SLACK_TEAM_ID", ""),
                "SLACK_CHANNEL_ID": os.getenv("SLACK_CHANNEL_ID", ""),
            },
        )

        transport = await self.exit_stack.enter_async_context(
            stdio_client(server_params)
        )
        read, write = transport
        self.session = await self.exit_stack.enter_async_context(
            ClientSession(read, write)
        )
        await self.session.initialize()

        # Discover tools
        response = await self.session.list_tools()
        self.tools = [
            {
                "name": tool.name,
                "description": tool.description or "",
                "input_schema": tool.inputSchema,
            }
            for tool in response.tools
        ]
        print(f"Connected. {len(self.tools)} tools available:", file=sys.stderr)
        for t in self.tools:
            print(f"  - {t['name']}", file=sys.stderr)

    async def chat(self, prompt: str, history: list) -> tuple[str, list]:
        """Send prompt to Claude with Slack MCP tools."""
        history.append({"role": "user", "content": prompt})

        while True:
            response = self.anthropic.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=4096,
                system=SYSTEM_PROMPT,
                messages=history,
                tools=self.tools,
            )

            assistant_content = list(response.content)
            history.append({"role": "assistant", "content": assistant_content})

            if response.stop_reason == "end_turn":
                text = "".join(
                    block.text for block in response.content if block.type == "text"
                )
                return text, history

            # Handle tool_use
            tool_results = []
            for block in response.content:
                if block.type != "tool_use":
                    continue

                print(f"  calling {block.name}...", file=sys.stderr)
                result = await self.session.call_tool(block.name, block.input)

                result_text = ""
                if result.content:
                    result_text = " ".join(
                        c.text for c in result.content if hasattr(c, "text")
                    )

                tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result_text,
                    }
                )

            history.append({"role": "user", "content": tool_results})

    async def cleanup(self):
        await self.exit_stack.aclose()


async def main():
    client = SlackMCPClient()

    print("Slack AI Assistant (MCP)")
    print("Connecting to Slack MCP server...", file=sys.stderr)

    try:
        await client.connect()
    except Exception as e:
        print(f"Failed to connect: {e}", file=sys.stderr)
        sys.exit(1)

    print("\nType your question, or 'quit' to exit.\n")
    history = []

    while True:
        try:
            prompt = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye!")
            break

        if not prompt:
            continue
        if prompt.lower() in ("quit", "exit", "q"):
            print("Bye!")
            break

        print("Thinking...", file=sys.stderr)
        try:
            text, history = await client.chat(prompt, history)
            print(f"\n{text}\n")
        except Exception as e:
            print(f"\nError: {e}\n", file=sys.stderr)

    await client.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
