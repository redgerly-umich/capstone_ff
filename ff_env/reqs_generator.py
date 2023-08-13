import os
import ast
import nbformat
import pkg_resources

# Built with support of GPT 3.5.
def extract_imports_from_code(code):
    tree = ast.parse(code)
    imports = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            module = node.module
            for alias in node.names:
                if module:
                    full_import = f"{module}.{alias.name}"
                else:
                    full_import = alias.name
                imports.append(full_import)

    return imports

def extract_imports_from_ipynb(ipynb_path):
    with open(ipynb_path, 'r') as nb_file:
        nb_content = nb_file.read()

    notebook = nbformat.reads(nb_content, as_version=4)

    imports = set()

    for cell in notebook.cells:
        if cell.cell_type == 'code':
            if not "%" in cell.source:
                imports_from_cell = extract_imports_from_code(cell.source)
                imports.update(imports_from_cell)

    return imports

def process_directory(directory_path, exclude_files=[]):
    imported_packages = {}

    for root, dirs, files in os.walk(directory_path):
        for file_name in files:
            if file_name.endswith('.py') or file_name.endswith('.ipynb'):
                if file_name not in exclude_files:
                    file_path = os.path.join(root, file_name)
                    if file_name.endswith('.py'):
                        imports = extract_imports_from_code(open(file_path, 'r').read())
                    elif file_name.endswith('.ipynb'):
                        imports = extract_imports_from_ipynb(file_path)
                    for package in imports:
                        if package not in imported_packages:
                            try:
                                version = pkg_resources.get_distribution(package).version
                                imported_packages[package] = version
                            except pkg_resources.DistributionNotFound:
                                imported_packages[package] = None

    return imported_packages

# Provide the starting directory
starting_directory = './'

# Specify the file names to exclude
files_to_exclude = ['dbConfigManager_Explore_Relationships.ipynb']

# Get imported packages with versions from all subdirectories
imported_packages = process_directory(starting_directory, exclude_files=files_to_exclude)

# Write imported packages and versions to requirements.txt
with open('requirements.txt', 'w') as req_file:
    for package, version in imported_packages.items():
        req_file.write(package)
        if version:
            req_file.write(f"=={version}")
        req_file.write('\n')
