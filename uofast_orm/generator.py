"""
ORM Class Generator
===================

Reads DICT definitions from a U2 file and uses Ollama/deepseek-coder to
generate Python model classes that inherit from UopyModel.

Usage:
    python -m uofast_orm.generator <file_name>
    python -m uofast_orm.generator CUSTOMERS
    python -m uofast_orm.generator CUSTOMERS -o models.py -c CustomerModel
"""

import sys
import argparse
from typing import Dict, List, Any, Optional
import requests
import uopy


class ORMClassGenerator:
    """
    Agent that generates ORM class definitions from U2 DICT files.

    Uses:
    - uopy to read DICT definitions directly
    - Ollama/deepseek-coder to generate Python code
    """

    def __init__(
        self,
        session: uopy.Session,
        ollama_host: str = "http://localhost:11434",
        model: str = "deepseek-coder:6.7b"
    ):
        """
        Initialize the generator.

        Args:
            session: Active uopy Session
            ollama_host: Ollama API host URL
            model: Ollama model to use for code generation
        """
        self.session = session
        self.ollama_host = ollama_host
        self.model = model

    def read_dict_definitions(self, file_name: str) -> List[Dict[str, Any]]:
        """
        Read DICT definitions for a file using uopy.

        Args:
            file_name: Name of the U2 file

        Returns:
            List of DICT item definitions
        """
        print(f"Reading DICT definitions for {file_name}...")

        dict_items = []

        try:
            # Select DICT items (D-type and V-type)
            select_cmd = f'SELECT DICT {file_name} WITH TYPE = "D" OR WITH TYPE = "V"'
            uopy.Command(select_cmd, session=self.session).run()

            # Get selected DICT item IDs
            select_list = uopy.List(0, session=self.session).read_list()

            if not select_list:
                print(f"Warning: No DICT items found for {file_name}")
                return []

            print(f"Found {len(select_list)} DICT items")

            # Open DICT file and read records
            with uopy.File(file_name, dict_flag=1, session=self.session) as dict_file:
                for dict_id in select_list:
                    try:
                        record = dict_file.read(dict_id)

                        record_list = list(record) if hasattr(record, '__iter__') else [str(record)]

                        dict_type = record_list[0] if len(record_list) > 0 else ""
                        field_number = record_list[1] if len(record_list) > 1 else ""
                        conversion = record_list[2] if len(record_list) > 2 else ""
                        heading = record_list[3] if len(record_list) > 3 else ""

                        dict_items.append({
                            "name": dict_id,
                            "type": dict_type,
                            "field_number": field_number,
                            "conversion": conversion,
                            "heading": heading
                        })

                    except Exception as e:
                        print(f"Warning: Could not read DICT item {dict_id}: {e}")
                        continue

            # Filter to only D-type fields (data fields)
            data_fields = [item for item in dict_items if item["type"].startswith("D")]

            print(f"Found {len(data_fields)} D-type (data) fields")

            return data_fields

        except Exception as e:
            print(f"Error reading DICT definitions: {e}")
            return []

    def generate_class_code(
        self,
        file_name: str,
        dict_items: List[Dict[str, Any]],
        class_name: Optional[str] = None
    ) -> str:
        """
        Generate Python class code using Ollama.

        Args:
            file_name: Name of the U2 file
            dict_items: List of DICT item definitions
            class_name: Optional class name (default: capitalized file_name)

        Returns:
            Generated Python code
        """
        if not class_name:
            class_name = self._generate_class_name(file_name)

        prompt = self._build_prompt(file_name, class_name, dict_items)

        print(f"\nGenerating class code using {self.model}...")

        try:
            response = requests.post(
                f"{self.ollama_host}/api/generate",
                json={
                    "model": self.model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": 0.3,
                        "top_p": 0.9
                    }
                },
                timeout=60
            )

            if response.status_code == 200:
                result = response.json()
                generated_code = result.get("response", "")
                return self._clean_generated_code(generated_code)
            else:
                print(f"Error from Ollama: {response.status_code}")
                print(response.text)
                return self._generate_fallback_code(file_name, class_name, dict_items)

        except requests.exceptions.RequestException as e:
            print(f"Error calling Ollama API: {e}")
            print("Falling back to template-based generation...")
            return self._generate_fallback_code(file_name, class_name, dict_items)

    def _generate_class_name(self, file_name: str) -> str:
        """Generate a Python class name from file name."""
        parts = file_name.lower().split('_')
        if len(parts) == 1:
            name = parts[0].capitalize()
            if name.endswith('s') and len(name) > 1:
                name = name[:-1]
            return name
        else:
            result = ''.join(p.capitalize() for p in parts)
            if result.endswith('s') and len(result) > 1:
                result = result[:-1]
            return result

    def _build_prompt(
        self,
        file_name: str,
        class_name: str,
        field_info: List[Dict[str, Any]]
    ) -> str:
        """Build the prompt for Ollama."""

        field_list = "\n".join([
            f"- {item['name']} (field {item['field_number']}): {item.get('heading', '')}"
            for item in field_info
        ])

        prompt = f"""Generate a Python class using the UopyModel ORM for a U2 Unidata file.

File Information:
- U2 File Name: {file_name}
- Python Class Name: {class_name}

DICT Fields:
{field_list}

Requirements:
1. Import UopyModel from uofast_orm
2. Create a class named {class_name} that inherits from UopyModel
3. Set _file_name = "{file_name}"
4. Set _field_names as a list of the DICT field names
5. Create a _field_map dictionary that maps Pythonic property names (snake_case) to DICT field names
6. Property names should be lowercase with underscores, descriptive, and follow Python conventions
7. Add a docstring to the class
8. Add type hints where appropriate
9. Only include the class definition code, no examples or usage

Generate ONLY the Python class code, nothing else. Do not include markdown formatting or explanations.
"""
        return prompt

    def _clean_generated_code(self, code: str) -> str:
        """Clean up generated code from Ollama."""
        if "```python" in code:
            code = code.split("```python")[1].split("```")[0]
        elif "```" in code:
            code = code.split("```")[1].split("```")[0]
        return code.strip()

    def _generate_fallback_code(
        self,
        file_name: str,
        class_name: str,
        field_info: List[Dict[str, Any]]
    ) -> str:
        """Generate code using a template when Ollama is unavailable."""

        field_names = [item["name"] for item in field_info]

        field_map = {}
        for item in field_info:
            dict_name = item["name"]
            prop_name = dict_name.lower().replace('-', '_').replace('.', '_').replace('@', '')
            field_map[prop_name] = dict_name

        field_names_str = ", ".join([f'"{name}"' for name in field_names])

        field_map_lines = []
        for prop_name, dict_name in field_map.items():
            field_map_lines.append(f'        "{prop_name}": "{dict_name}"')
        field_map_str = ",\n".join(field_map_lines)

        code = f'''from uofast_orm import UopyModel
from typing import Optional


class {class_name}(UopyModel):
    """
    ORM model for {file_name} file.

    Auto-generated from DICT definitions.
    """
    _file_name = "{file_name}"
    _field_names = [{field_names_str}]
    _field_map = {{
{field_map_str}
    }}
'''
        return code

    def generate_and_save(
        self,
        file_name: str,
        output_file: Optional[str] = None,
        class_name: Optional[str] = None
    ) -> str:
        """
        Generate ORM class and optionally save to file.

        Args:
            file_name: Name of the U2 file
            output_file: Optional output file path
            class_name: Optional class name

        Returns:
            Generated code
        """
        dict_items = self.read_dict_definitions(file_name)

        if not dict_items:
            print("No DICT items found. Cannot generate class.")
            return ""

        code = self.generate_class_code(file_name, dict_items, class_name)

        if output_file:
            with open(output_file, 'w') as f:
                f.write(code)
            print(f"\nGenerated class saved to: {output_file}")

        return code


def main():
    """Main entry point for the ``uofast-generate`` CLI command."""
    parser = argparse.ArgumentParser(
        description="Generate ORM class from U2 DICT definitions"
    )
    parser.add_argument(
        "file_name",
        help="U2 file name to generate class for"
    )
    parser.add_argument(
        "-o", "--output",
        help="Output file path (default: <file_name>_model.py)"
    )
    parser.add_argument(
        "-c", "--class-name",
        help="Custom class name (default: auto-generated from file name)"
    )
    parser.add_argument(
        "--host",
        help="U2 server hostname or IP"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=31438,
        help="U2 server port (default: 31438)"
    )
    parser.add_argument(
        "--user",
        help="U2 username"
    )
    parser.add_argument(
        "--password",
        help="U2 password"
    )
    parser.add_argument(
        "--account",
        help="U2 account path"
    )
    parser.add_argument(
        "--service",
        default="udcs",
        help="U2 service name (default: udcs)"
    )
    parser.add_argument(
        "--ollama-host",
        default="http://localhost:11434",
        help="Ollama API host URL (default: http://localhost:11434)"
    )
    parser.add_argument(
        "--model",
        default="deepseek-coder:6.7b",
        help="Ollama model to use (default: deepseek-coder:6.7b)"
    )

    args = parser.parse_args()

    if not args.output:
        args.output = f"{args.file_name.lower()}_model.py"

    if not all([args.host, args.user, args.password, args.account]):
        print("Error: --host, --user, --password, and --account are required.")
        sys.exit(1)

    print("Connecting to U2 database...")
    config = {
        'user': args.user,
        'password': args.password,
        'service': args.service,
        'account': args.account,
        'host': args.host,
        'port': args.port,
    }

    session = None
    try:
        session = uopy.connect(**config)

        generator = ORMClassGenerator(
            session=session,
            ollama_host=args.ollama_host,
            model=args.model
        )

        code = generator.generate_and_save(
            file_name=args.file_name,
            output_file=args.output,
            class_name=args.class_name
        )

        print("\n" + "=" * 70)
        print("GENERATED CODE")
        print("=" * 70)
        print(code)
        print("=" * 70)

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if session:
            try:
                session.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
