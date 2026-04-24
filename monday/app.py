#!/usr/bin/env python3
"""CLI tool — query Monday.com via MCP + Claude tool_use."""

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
MONDAY_WORKSPACE_ID = os.getenv("MONDAY_WORKSPACE_ID", "")

SYSTEM_PROMPT = (
    "You are a Monday.com assistant. Use the available Monday.com tools to answer user questions. "
    "Be concise. Use tables/lists where helpful. Answer in the same language as the user."
    + (
        f"\n\nIMPORTANT: Only operate within workspace ID {MONDAY_WORKSPACE_ID}. "
        f"When listing boards, always pass workspace_ids=[{MONDAY_WORKSPACE_ID}]. "
        "Do not access boards outside this workspace."
        if MONDAY_WORKSPACE_ID
        else ""
    )
)


class MCPClient:
    def __init__(self):
        self.exit_stack = AsyncExitStack()
        self.session: ClientSession | None = None
        self.anthropic = Anthropic()
        self.tools = []

    async def connect(self):
        """Spawn Monday MCP server and connect via stdio."""
        server_params = StdioServerParameters(
            command="npx",
            args=["-y", "verdant-monday-mcp"],
            env={
                **os.environ,
                "MONDAY_API_KEY": os.getenv("MONDAY_API_KEY", ""),
                "MONDAY_WORKSPACE_NAME": os.getenv("MONDAY_WORKSPACE_NAME", ""),
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

        # Discover and convert tools to Anthropic format
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
        """Send prompt to Claude with MCP tools. Returns (response_text, updated_history)."""
        history.append({"role": "user", "content": prompt})

        while True:
            response = self.anthropic.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=4096,
                system=SYSTEM_PROMPT,
                messages=history,
                tools=self.tools,
            )

            # Collect assistant content
            assistant_content = list(response.content)
            history.append({"role": "assistant", "content": assistant_content})

            if response.stop_reason == "end_turn":
                # Extract final text
                text = "".join(
                    block.text for block in response.content if block.type == "text"
                )
                return text, history

            # Handle tool_use blocks
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
    client = MCPClient()

    print("Monday.com AI Assistant (MCP)")
    print("Connecting to Monday MCP server...", file=sys.stderr)

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
