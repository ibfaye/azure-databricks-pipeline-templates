# Contributing

Thanks for your interest in contributing! This project is maintained by [Sen'Analytics](https://senanalytics.com) and welcomes contributions from the community.

## Development Setup

```bash
git clone https://github.com/ibfaye/azure-databricks-pipeline-templates.git
cd azure-databricks-pipeline-templates
```

### Terraform Development

```bash
cd terraform
terraform init -backend=false   # Init without remote state for local dev
terraform fmt -recursive        # Format all files
terraform validate              # Validate syntax
tflint --recursive              # Lint
```

### dbt Development

```bash
cd dbt
pip install dbt-databricks
dbt deps
dbt compile --target dev
```

### Python SDK Development

```bash
cd pipelines
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Pull Request Process

1. Fork the repo and create a feature branch
2. Make your changes
3. Run `terraform fmt -recursive && terraform validate` in the `terraform/` directory
4. Run `dbt compile` in the `dbt/` directory
5. Push and open a PR against `main`
6. CI will check: Terraform format/validate/lint + dbt compile

## Adding a New Module

1. Create directory under `terraform/modules/` with `main.tf`, `variables.tf`, `outputs.tf`
2. Update root `terraform/main.tf` to reference the module
3. Add outputs to `terraform/outputs.tf`
4. Document in `docs/architecture.md`

## Code Style

- **Terraform**: snake_case resource names, 2-space indent, comments for non-obvious blocks
- **dbt**: SQLFluff-style formatting, lowercase keywords, trailing commas
- **Python**: PEP 8, type hints, docstrings on public functions

## Questions?

Open an issue or contact iboufaye2000@hotmail.com.
