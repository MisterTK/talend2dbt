# Talend to DBT Migration Tool


AI driven conversion of Talend ETL jobs into production-ready DBT models for BigQuery.

## Quick Start

```bash
# 1. Bootstrap environment
/00-bootstrap

# 2. Pre-process Talend jobs
/01-pre-process <path/to/talend/jobs> [output_path]

# 3. Generate DBT project
/02-migrate <processed_talend_path> [dbt_output_path]

# 4. Validate and format
/03-post-process <dbt_project_path>
```

## Features

- **98%+ SQL generation coverage** - Complete business logic translation
- **70-85% model reduction** - Intelligent consolidation of Talend jobs
- **Medallion architecture** - Bronze (staging) → Silver (intermediate) → Gold (marts)
- **BigQuery optimized** - Latest 2025 syntax and best practices
- **Token-efficient parsing** - LLM-optimized Talend extraction

## Requirements

- Python 3.11+
- DBT 1.10+
- BigQuery target platform

## Architecture

### Three-Phase Pipeline

1. **Pre-Processing** - Extract and optimize Talend XML into LLM-friendly JSON
2. **Migration** - Generate complete DBT project with medallion structure
3. **Post-Processing** - Format, lint, and validate output

## Project Structure

```
talend2dbt/
├── .claude/commands/     # Custom slash commands
├── talend_parser/        # Core extraction logic
├── standards/            # Migration standards (13 files)
```

## Quality Targets

| Metric | Target |
|--------|--------|
| SQL Coverage | ≥98% |
| Model Reduction | 70-85% |
| Business Logic Preservation | 100% |
| Manual Intervention | <2% |

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

For issues and questions, please open an issue on GitHub.

## License

MIT License - see [LICENSE](LICENSE) file for details.
