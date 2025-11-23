import json
import ast

def verify_notebook(path):
    try:
        with open(path, 'r') as f:
            nb = json.load(f)
        print(f"Notebook {path} is valid JSON.")
        
        for i, cell in enumerate(nb['cells']):
            if cell['cell_type'] == 'code':
                source = "".join(cell['source'])
                try:
                    ast.parse(source)
                except SyntaxError as e:
                    print(f"Syntax error in cell {i}: {e}")
                    return False
        print("All code cells have valid Python syntax.")
        return True
    except Exception as e:
        print(f"Error verifying notebook: {e}")
        return False

verify_notebook('examples/neutron_demo.ipynb')
