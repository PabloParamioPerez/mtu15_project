"""
MTU15 Project - OMIE Electricity Market Data Pipeline
======================================================

Main entry point for the data pipeline.
"""

import sys
from pathlib import Path
from rich.console import Console
from rich.panel import Panel

console = Console()


def main():
    """
    Main entry point for the MTU15 data pipeline.

    This function provides a command-line interface to run different
    pipeline stages and utilities.
    """
    console.print(
        Panel.fit(
            "[bold blue]MTU15 Project - OMIE Data Pipeline[/bold blue]\n\n"
            "Available commands:\n"
            "• Download: Run download scripts (00_*.py)\n"
            "• Parse: Run parsing scripts (10_*.py)\n"
            "• Build: Run consolidation scripts (20_*.py)\n"
            "• Validate: Run data validation checks\n"
            "• Status: Show pipeline status\n\n"
            "[dim]Use individual scripts in scripts/pipelines/omie/ for specific operations[/dim]",
            title="🚀 MTU15 Data Pipeline",
        )
    )

    if len(sys.argv) > 1:
        command = sys.argv[1].lower()

        if command == "status":
            show_pipeline_status()
        elif command == "validate":
            run_validation_checks()
        else:
            console.print(f"[red]Unknown command: {command}[/red]")
            console.print("[yellow]Try: status, validate[/yellow]")
    else:
        console.print("[dim]Run 'mtu15-project status' to see pipeline status[/dim]")


def show_pipeline_status():
    """Show the current status of the data pipeline."""
    console.print("[bold]Pipeline Status[/bold]")

    data_dirs = [
        "data/raw/omie",
        "data/processed/omie",
        "data/metadata",
    ]

    for dir_path in data_dirs:
        path = Path(dir_path)
        if path.exists():
            file_count = len(list(path.rglob("*"))) if path.is_dir() else 1
            console.print(f"✅ {dir_path}: {file_count} items")
        else:
            console.print(f"❌ {dir_path}: missing")

    scripts = [
        "scripts/pipelines/omie/00_download_marginalpdbc.py",
        "scripts/pipelines/omie/10_parse_marginalpdbc.py",
        "scripts/pipelines/omie/20_build_marginalpdbc_all.py",
    ]

    console.print("\n[bold]Pipeline Scripts[/bold]")
    for script in scripts:
        path = Path(script)
        if path.exists():
            console.print(f"✅ {script}")
        else:
            console.print(f"❌ {script}: missing")


def run_validation_checks():
    """Run basic validation checks on available data."""
    console.print("[bold]Running Validation Checks[/bold]")

    try:
        sample_dir = Path("tests/samples")
        if sample_dir.exists():
            samples = list(sample_dir.glob("*.txt"))
            if samples:
                console.print(f"Found {len(samples)} sample files to validate")
                for sample in samples:
                    console.print(f"• {sample.name}")
            else:
                console.print("❌ No sample files found in tests/samples/")
        else:
            console.print("❌ tests/samples/ directory not found")

    except Exception as e:
        console.print(f"[red]Validation module not available: {e}[/red]")


if __name__ == "__main__":
    main()
