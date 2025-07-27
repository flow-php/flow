# Examples

This document provides comprehensive guidance for working with Flow PHP examples, including how to add new examples,
update existing ones, and run them effectively.

## Overview

Flow PHP includes a comprehensive collection of executable examples organized by topic, demonstrating all major
framework features. Each example is self-contained with isolated dependencies and serves as both documentation and
integration testing.

## Directory Structure

Examples are organized hierarchically under `/examples/topics/` with the following structure:

```
examples/
├── run.php              # Main execution script
├── clean.php            # Cleanup utility
└── topics/              # Topic-based organization
    ├── aggregations/    # Data aggregation operations (8 examples)
    ├── data_frame/      # DataFrame operations (7 examples)
    ├── data_reading/    # Data input methods (13 examples)
    ├── data_writing/    # Data output methods (9 examples)
    ├── filesystem/      # Filesystem operations (4 examples)
    ├── join/            # Data joining (2 examples)
    ├── partitioning/    # Data partitioning (4 examples)
    ├── schema/          # Schema management (4 examples)
    ├── transformations/ # Data transformations (12 examples)
    ├── types/           # Type system (2 examples)
    └── window_functions/ # Window functions (1 example)
```

### Standard Example Structure

Each example follows this consistent structure:

```
examples/topics/{topic}/{example}/
├── code.php                    # Main executable script (required)
├── composer.json               # Isolated dependencies (required)
├── composer.lock               # Locked dependencies (generated)
├── output.txt                  # Expected output (required)
├── description.md              # Human-readable explanation (optional)
├── priority.txt                # Numeric priority for ordering (optional)
├── hidden.txt                  # Hide from website (optional)
├── input/                      # Sample input data (optional)
├── output/                     # Generated output files (optional)
├── vendor/                     # Composer dependencies (generated)
└── flow_php_example.zip        # Distribution archive (generated)
```

## Running Examples

### Command Line Interface

Use the main execution script to run examples:

```bash
# Run all examples
./examples/run.php

# Run specific topic
./examples/run.php --topic=data_reading

# Run specific example
./examples/run.php --topic=data_reading --example=csv

# Update dependencies instead of install
./examples/run.php --composer-update

# Generate distribution archives
./examples/run.php --composer-archive
```

### Available Options

- `--topic (-t)`: Run examples from a specific topic
- `--example (-e)`: Run a specific example (requires --topic)
- `--composer-update (-u)`: Update dependencies instead of install
- `--composer-archive (-a)`: Generate ZIP archives for distribution

### Integration with Build System

Examples are integrated into the build process:

```bash
# Run examples as part of test suite
composer test:examples

# Run full test suite including examples
composer test
```

Examples run automatically in CI/CD on changes to `examples/**` directory.

## Adding New Examples

### Step-by-Step Process

1. **Choose Topic and Name**
    - Use existing topics when possible
    - Create new topics only for distinct feature areas
    - Use descriptive, lowercase names with underscores

2. **Create Directory Structure**
   ```bash
   mkdir -p examples/topics/{topic}/{example}
   cd examples/topics/{topic}/{example}
   ```

3. **Create Required Files**

   **`code.php`** - Main executable script:
   ```php
   <?php
   declare(strict_types=1);

   use function Flow\ETL\DSL\{data_frame, /* other functions */};
   use function Flow\ETL\Adapter\[Adapter]\{/* adapter functions */};

   require __DIR__ . '/vendor/autoload.php';

   data_frame()
       ->read(/* extractor */)
       ->transform(/* transformations */)
       ->write(/* loader */)
       ->run();
   ```

   **`composer.json`** - Dependencies:
   ```json
   {
     "name": "flow-php/examples",
     "description": "Flow PHP - Examples",
     "license": "MIT",
     "type": "library",
     "require": {
       "flow-php/etl": "1.x-dev",
       "flow-php/etl-adapter-[specific]": "1.x-dev"
     },
     "archive": {
       "exclude": [".env", "vendor"]
     }
   }
   ```

   **`priority.txt`** - Numeric priority (1-99, lower = higher priority):
   ```
   10
   ```

4. **Add Optional Files**

   **`description.md`** - Human-readable explanation:
   ```markdown
   # Example Title

   Brief description of what this example demonstrates.

   ## Key Features

   - Feature 1
   - Feature 2

   ## Use Cases

   When to use this pattern...
   ```

   **`input/`** directory - Sample data files if needed
   **`hidden.txt`** - Empty file to hide from website

5. **Test the Example**
   ```bash
   # Test locally
   ./examples/run.php --topic={topic} --example={example}
   ```

## Updating Existing Examples

### Common Update Scenarios

1. **Code Changes**
    - Edit `code.php` following existing patterns
    - Test thoroughly with `./examples/run.php`
    - Update `output.txt` if output changes

2. **Dependency Updates**
    - Modify `composer.json` as needed
    - Run `./examples/run.php --composer-update --topic={topic} --example={example}` to refresh dependencies
    - Commit both `composer.json` and `composer.lock`

3. **Documentation Updates**
    - Edit `description.md` for clarity
    - Keep documentation concise and focused

4. **Priority Changes**
    - Modify `priority.txt` value (1-99)
    - Lower numbers appear first in listings

## Priority and Organization System

### Priority System

Examples use numeric priorities for ordering:

- **Range**: 1-99 (lower numbers = higher priority)
- **Default**: 99 if `priority.txt` doesn't exist
- **Application**: Both topics and individual examples
- **Display**: Lower priority numbers appear first

### Visibility Control

- **`hidden.txt`**: Empty file that hides examples from website
- **Use cases**: Internal examples, development utilities, deprecated examples
- **Current usage**: Applied to some sequence generator examples

## Maintenance and Cleanup

### Cleanup Operations

Remove generated files and dependencies:

```bash
# Clean all examples
./examples/clean.php

# This removes:
# - vendor/ directories
# - flow_php_example.zip files
```

### Archive Generation

Create distribution packages:

```bash
# Generate archives for all examples
./examples/run.php --composer-archive

# Archives are created as flow_php_example.zip in each example directory
```

## Integration with Website

Examples are automatically integrated into the Flow PHP website:

- **Dynamic content**: Website reads examples directly from filesystem
- **Priority ordering**: Uses `priority.txt` for display order
- **Visibility**: Respects `hidden.txt` files
- **Multi-format**: Supports various output formats (txt, xml, csv, json)
- **Code highlighting**: Automatic syntax highlighting for code blocks