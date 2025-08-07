import argparse
import importlib
import os
import sys
from pathlib import Path


def discover_commands():
    """
    Discovers all Python files in cwms/utils/cli/ and returns a dict of command -> module
    """
    base_dir = Path(__file__).parent
    commands = {}

    for file in os.listdir(base_dir):
        # Only call .py scripts and ignore any dunder files (i.e. starts with _)
        if file.endswith(".py") and not file.startswith("_"):
            cmd = file[:-3]
            mod_path = f"cwms.utils.cli.{cmd}"
            commands[cmd] = mod_path

    return commands


def main():
    commands = discover_commands()

    parser = argparse.ArgumentParser(
        prog="cwms", description="CWMS-Python CLI Utilities"
    )
    parser.add_argument("command", choices=commands.keys(), help="The script to run")
    parser.add_argument(
        "args", nargs=argparse.REMAINDER, help="Arguments to pass to the script"
    )

    # Ensures that at least one argument (the command) is provided otherwise provides the cwms help
    if len(sys.argv) < 2:
        parser.print_help()
        sys.exit(1)

    parsed = parser.parse_args()
    command = parsed.command
    args = parsed.args

    # Loads the selected command module and calls its main function
    try:
        mod = importlib.import_module(commands[command])
        if hasattr(mod, "main"):
            mod.main(args)
        else:
            print(
                f"The module {commands[command]} does not have a 'main(args)' function."
            )
            sys.exit(2)
    except ImportError as e:
        print(f"Failed to import command module '{command}': {e}")
        sys.exit(1)
