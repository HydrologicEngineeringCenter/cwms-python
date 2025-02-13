import ast
import json
import os
import re

import black
import nbformat
import pandas as pd

# Base directories
TESTS_DIR = "./tests/"
EXAMPLES_DIR = "./examples/"

# Ensure examples directory exists
os.makedirs(EXAMPLES_DIR, exist_ok=True)

GITHUB_BASE_URL = "https://github.com/HydrologicEngineeringCenter/cwms-python/tree/main"

# Ignore these directories when generating examples
IGNORE_DIR_NAMES = [TESTS_DIR, "/resources/", "/__pycache__"]


def extract_api_calls_and_mock_responses(test_file_path):
    """
    Parses a test file, extracts API calls, mock API responses, and generates notebook-friendly code.
    """
    with open(test_file_path, "r", encoding="utf-8") as file:
        source_code = file.read()

    # Parse the Python test file
    tree = ast.parse(source_code)

    setup_code = []
    function_contents = {}
    resource_data = {}  # Store JSON file contents used in mocks
    mock_paths = {}  # Store API paths and method types

    # Extract import statements and function bodies
    for node in tree.body:

        if isinstance(node, ast.Import) or isinstance(node, ast.ImportFrom):
            # If the import has the word test in it, skip it
            if re.search(r"test", ast.unparse(node)):
                continue
            setup_code.append(ast.unparse(node))

        elif isinstance(node, ast.Assign):  # Look for variable assignments
            for target in node.targets:
                if isinstance(target, ast.Name) and isinstance(node.value, ast.Call):
                    if (
                        isinstance(node.value.func, ast.Name)
                        and node.value.func.id == "read_resource_file"
                    ):
                        filename = node.value.args[
                            0
                        ].value  # Extract the filename being loaded
                        resource_data[target.id] = (
                            filename  # Store variable name and JSON file
                        )

        elif isinstance(node, ast.Expr):  # Check for mock API paths
            call_code = ast.unparse(node).strip()
            match = re.search(r"requests_mock", call_code)
            if match:
                continue  # Ignore requests_mock calls

        elif isinstance(node, ast.FunctionDef) and node.name.startswith("test_"):
            function_code = []
            for sub_node in node.body:
                call_code = ast.unparse(sub_node).strip()
                if isinstance(sub_node, ast.Expr):  # Function call expressions
                    if not re.search(
                        r"requests_mock", call_code
                    ):  # Ignore requests_mock
                        function_code.append(call_code)
                elif isinstance(
                    sub_node,
                    (ast.Assign, ast.Assert, ast.If, ast.For, ast.While, ast.Try),
                ):
                    if re.search(r"requests_mock", call_code):  # Ignore requests_mock
                        continue
                    function_code.append(call_code)
            function_contents[node.name] = function_code
    return setup_code, function_contents, resource_data, mock_paths


def load_mock_data(resource_data):
    """
    Reads mock JSON files from 'tests/resources/' and returns their contents.
    """
    mock_outputs = {}
    for var_name, filename in resource_data.items():
        file_path = os.path.join(
            "tests/resources/", filename
        )  # Assuming mock files are stored here
        try:
            with open(file_path, "r", encoding="utf-8") as json_file:
                data = json.load(json_file)
                mock_outputs[var_name] = data
        except FileNotFoundError:
            print(f"⚠️ Warning: Resource file not found: {file_path}")
            mock_outputs[var_name] = None
    return mock_outputs


import os

import nbformat
import pandas as pd


def generate_notebooks():
    """
    Recursively scans TESTS_DIR, processes test files, and generates corresponding Jupyter notebooks.
    """
    for root, _, files in os.walk(TESTS_DIR):
        # Skip exact matches of ignored directories
        if any(root.endswith(ignore) for ignore in IGNORE_DIR_NAMES):
            print("⚠️\tIgnoring ", root)
            continue
        for file in files:
            if file.endswith("_test.py"):
                test_file_path = os.path.join(root, file)
                setup_code, function_contents, resource_data, mock_paths = (
                    extract_api_calls_and_mock_responses(test_file_path)
                )

                if function_contents:
                    # Load mock responses
                    mock_outputs = load_mock_data(resource_data)

                    # Create a matching notebook filename
                    notebook_name = file.replace("_test.py", ".ipynb")

                    # Preserve directory structure in examples/
                    relative_path = os.path.relpath(root, TESTS_DIR)
                    test_github_link = f"{GITHUB_BASE_URL}{test_file_path}".replace(
                        "\\", "/"
                    ).replace("main.", "main")

                    output_dir = os.path.join(EXAMPLES_DIR, relative_path)
                    os.makedirs(output_dir, exist_ok=True)

                    notebook_path = os.path.join(output_dir, notebook_name)

                    # Create Notebook cells
                    notebook_cells = []
                    # Build our example page title
                    last_dir = os.path.basename(os.path.dirname(test_file_path))
                    formatted_title = last_dir.replace("_", " ").title()
                    notebook_cells.append(
                        nbformat.v4.new_markdown_cell(f"# {formatted_title} Examples\n")
                    )
                    notebook_cells.append(
                        nbformat.v4.new_markdown_cell(
                            f"**Example generated from:** [{test_github_link.replace(GITHUB_BASE_URL, "")}]({test_github_link})"
                        )
                    )
                    if setup_code:
                        notebook_cells.append(
                            nbformat.v4.new_code_cell(
                                "\n".join(
                                    ["from cwms.api import init_session"] + setup_code
                                )
                            )
                        )

                    # Init database/write access section
                    notebook_cells.append(
                        nbformat.v4.new_markdown_cell(
                            "### Initializing the database and write access\n"
                            "cwms-python will connect by default to the USACE public database available through "
                            "[CWMS Data](https://cwms-data.usace.army.mil/cwms-data/). \n"
                            "\nhttps://cwms-data.usace.army.mil/cwms-data/\n\n"
                            "The apiRoot can be updated to access data directly from a USACE district database.\n"
                            "* Endpoints on the [Swagger Docs page](https://cwms-data.usace.army.mil/cwms-data/swagger-ui.html) "
                            "with a 🔒 icon require the apiKey be set."
                        )
                    )
                    notebook_cells.append(
                        nbformat.v4.new_code_cell(
                            'api_root = "https://cwms-data-test.cwbi.us/cwms-data/"\n'
                            "api = init_session(api_root=api_root)"
                        )
                    )

                    # Organize function calls by operation type
                    categorized_functions = {
                        "GET Requests": {},
                        "POST Requests": {},
                        "STORE Requests": {},
                        "DELETE Requests": {},
                        "PATCH Requests": {},
                        "Other API Calls": {},
                    }

                    for function_name, statements in function_contents.items():
                        function_code = "\n".join(
                            statements
                        )  # Keep full function contents as a single block
                        if ".get" in function_code:
                            categorized_functions["GET Requests"][
                                function_name
                            ] = function_code
                        elif ".post" in function_code:
                            categorized_functions["POST Requests"][
                                function_name
                            ] = function_code
                        elif ".store" in function_code:
                            categorized_functions["STORE Requests"][
                                function_name
                            ] = function_code
                        elif ".delete" in function_code:
                            categorized_functions["DELETE Requests"][
                                function_name
                            ] = function_code
                        elif ".patch" in function_code:
                            categorized_functions["PATCH Requests"][
                                function_name
                            ] = function_code
                        else:
                            categorized_functions["Other API Calls"][
                                function_name
                            ] = function_code

                    # Add categorized function calls with function-specific subheadings
                    for category, functions in categorized_functions.items():
                        if functions:
                            notebook_cells.append(
                                nbformat.v4.new_markdown_cell(f"# {category}")
                            )

                            for function_name, function_code in functions.items():
                                notebook_cells.append(
                                    nbformat.v4.new_markdown_cell(
                                        f"## {function_name.replace("_", ' ').replace("test ", '')}"
                                    )
                                )

                                # Check if function uses a mock data variable and replace it inline
                                modified_function_code = function_code
                                # Only set the input data if we are doing a store call
                                if "create" in function_name:
                                    for var_name, data in mock_outputs.items():
                                        if var_name in function_code:
                                            modified_function_code = (
                                                modified_function_code.replace(
                                                    var_name, "data"
                                                )
                                            )

                                            # Insert the actual data dictionary as an initialization step
                                            data_input = black.format_str(
                                                str(data), mode=black.Mode()
                                            )
                                            notebook_cells.append(
                                                nbformat.v4.new_code_cell(
                                                    f"# Input Data:\n"
                                                    f"data = {black.format_str(str(data), mode=black.Mode())}\n"
                                                )
                                            )
                                modified_function_code = black.format_str(
                                    modified_function_code, mode=black.Mode()
                                )
                                modified_function_code = modified_function_code.replace(
                                    "data = data\n", ""
                                )
                                # Do not show assert calls in code
                                no_assert_code = re.sub(
                                    r"assert .*", "", modified_function_code
                                )
                                # Remove extra new lines at the end
                                no_assert_code = re.sub(r"\n\n+", "\n", no_assert_code)
                                notebook_cells.append(
                                    nbformat.v4.new_code_cell(no_assert_code)
                                )

                                # Replace assert calls with a suitable code cell
                                # But only if there is assert data.json, assert data.df, or assert values
                                # i.e. the regex should look for these things

                                # Extract only relevant assert statements
                                assert_calls = re.findall(
                                    r"assert (data\.json|data\.df|values|type\(data\))",
                                    modified_function_code,
                                )

                                # Generate a separate code block for each valid assert statement
                                for assert_call in assert_calls:
                                    notebook_cells.append(
                                        nbformat.v4.new_code_cell(
                                            assert_call.replace("type(data)", "data.df")
                                        )
                                    )

                    # Create and save the notebook
                    notebook = nbformat.v4.new_notebook()
                    notebook.cells = notebook_cells

                    with open(notebook_path, "w", encoding="utf-8") as nb_file:
                        nbformat.write(notebook, nb_file)

                    print(f"✅\tNotebook saved: {notebook_path}")


if __name__ == "__main__":
    generate_notebooks()
