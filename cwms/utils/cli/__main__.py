import argparse
import importlib
import inspect
import sys
from pathlib import Path


def discover_commands() -> dict[str, str]:
    """
    Discover scripts under cwms/utils/cli:

    - Any top-level *.py file (not starting with '_')
    - Any immediate subdirectory that contains __init__.py

    Returns: dict of {command_name: module_import_path}
    """
    # Root of the cli directory
    base_dir = Path(__file__).parent
    base_pkg = __package__ or "cwms.utils.cli"

    commands: dict[str, str] = {}

    # Use any named python files in the root of the cli directory
    for p in base_dir.iterdir():
        if p.is_file() and p.suffix == ".py" and not p.name.startswith("_"):
            cmd = p.stem
            if cmd:
                commands[cmd] = f"{base_pkg}.{cmd}"

    # Grab sub packages within the cli directory, i.e. __init__.py within dirs
    for p in base_dir.iterdir():
        if p.is_dir() and not p.name.startswith("_"):
            if (p / "__init__.py").is_file():
                # Prefer package over file if both exist with same name
                commands[p.name] = f"{base_pkg}.{p.name}"

    return commands


def main():
    commands = discover_commands()
    if not commands:
        print("No commands found under cwms.utils.cli")
        sys.exit(1)

    command_names = sorted(commands.keys())

    parser = argparse.ArgumentParser(
        prog="cwms",
        description="CWMS CLI Utilities",
        epilog="Use 'cwms <command> --help' for more information on a command.",
    )
    parser.add_argument("command", choices=command_names, help="Command to run")
    # Catch all for unparsed arguments to pass through
    parser.add_argument("remainder", nargs=argparse.REMAINDER, help=argparse.SUPPRESS)

    if len(sys.argv) < 2:
        parser.print_help()
        sys.exit(1)

    parsed = parser.parse_args()
    command = parsed.command
    remainder = parsed.remainder

    mod_path = commands[command]
    try:
        mod = importlib.import_module(mod_path)
    except ImportError as e:
        print(f"Failed to import command module '{mod_path}': {e}")
        sys.exit(1)

    fn = getattr(mod, "main", None)
    if not callable(fn):
        print(f"The module {mod_path} does not have a callable 'main' function.")
        sys.exit(2)

    # Pass in the remainder args
    sys.argv = [f"{parser.prog} {command}"] + remainder

    # Call main() with appropriate signature
    # We need this because some commands may expect their arguments in a specific format
    try:
        sig = inspect.signature(fn)
        if len(sig.parameters) == 0:
            fn()
        else:
            # If they prefer main(args: list[str]) style, pass the remainder.
            fn(remainder)
    except SystemExit:
        # Let subcommand's argparse exit codes bubble up cleanly.
        raise
    except Exception as e:
        print(f"Error while running '{command}': {e}")
        sys.exit(1)
