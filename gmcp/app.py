#!/usr/bin/env python3
"""
GMCP — Google MCP CLI Assistant
Natural language CLI for Gmail + Google Drive, powered by Claude.

Usage:
    python app.py "search my unread emails"
    python app.py "list recent drive files"
    python app.py                              # interactive mode
"""

import json
import os
import sys

from dotenv import load_dotenv
load_dotenv()

import anthropic
from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel

from google_tools import TOOL_DEFINITIONS, TOOL_FUNCTIONS

console = Console()

MODEL = "claude-sonnet-4-20250514"
SYSTEM_PROMPT = """You are a helpful assistant with access to Gmail and Google Drive tools.
Use the provided tools to fulfill user requests about their email and files.
Always confirm before sending emails (prefer creating drafts unless explicitly asked to send).
When showing results, format them clearly.
Be concise."""


def get_client():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        console.print("[red]ANTHROPIC_API_KEY not set.[/red]")
        console.print("Export it: export ANTHROPIC_API_KEY=your-key")
        sys.exit(1)
    return anthropic.Anthropic(api_key=api_key)


def execute_tool(name: str, input_args: dict) -> str:
    """Execute a tool function and return result as string."""
    fn = TOOL_FUNCTIONS.get(name)
    if not fn:
        return f"Unknown tool: {name}"
    try:
        result = fn(**input_args)
        return result
    except Exception as e:
        return f"Error executing {name}: {e}"


def run_conversation(client: anthropic.Anthropic, user_input: str, history: list):
    """Run a full conversation turn with tool use loop."""
    history.append({"role": "user", "content": user_input})

    while True:
        response = client.messages.create(
            model=MODEL,
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            tools=TOOL_DEFINITIONS,
            messages=history,
        )

        # Collect assistant message
        assistant_content = response.content
        history.append({"role": "assistant", "content": assistant_content})

        # Check if we need to execute tools
        tool_uses = [block for block in assistant_content if block.type == "tool_use"]

        if not tool_uses:
            # No more tools — extract text response
            text_parts = [block.text for block in assistant_content if block.type == "text"]
            return "\n".join(text_parts)

        # Execute all tool calls and add results
        tool_results = []
        for tool_use in tool_uses:
            console.print(f"  [dim]→ calling {tool_use.name}...[/dim]")
            result = execute_tool(tool_use.name, tool_use.input)
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": tool_use.id,
                "content": result,
            })

        history.append({"role": "user", "content": tool_results})


def interactive_mode(client: anthropic.Anthropic):
    """Run interactive REPL mode."""
    console.print(Panel(
        "[bold green]GMCP[/bold green] — Google MCP Assistant\n"
        "Talk naturally about your Gmail & Google Drive.\n"
        "Type [bold]quit[/bold] or [bold]exit[/bold] to leave.",
        title="🔧 GMCP",
        border_style="blue",
    ))

    history = []
    while True:
        try:
            user_input = console.input("\n[bold cyan]You:[/bold cyan] ").strip()
        except (EOFError, KeyboardInterrupt):
            console.print("\n[dim]Bye![/dim]")
            break

        if not user_input:
            continue
        if user_input.lower() in ("quit", "exit", "q"):
            console.print("[dim]Bye![/dim]")
            break

        with console.status("[bold green]Thinking..."):
            try:
                response = run_conversation(client, user_input, history)
            except Exception as e:
                console.print(f"[red]Error: {e}[/red]")
                continue

        console.print()
        console.print(Markdown(response))


def single_mode(client: anthropic.Anthropic, query: str):
    """Run single query mode."""
    history = []
    with console.status("[bold green]Processing..."):
        response = run_conversation(client, query, history)
    console.print(Markdown(response))


def main():
    client = get_client()

    if len(sys.argv) > 1:
        query = " ".join(sys.argv[1:])
        single_mode(client, query)
    else:
        interactive_mode(client)


if __name__ == "__main__":
    main()
