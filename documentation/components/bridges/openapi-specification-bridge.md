# OpenAPI Specification Bridge

- [⬅️️ Back](../../introduction.md)
- [📚API Reference](/documentation/api/bridge/openapi/specification)
- [📁Files](/documentation/api/bridge/openapi/specification/indices/files.html)

This package provides bidirectional conversion between Flow PHP schemas and OpenAPI 3.0 specifications. 
It enables you to generate OpenAPI documentation from Flow schemas and vice versa, facilitating API-first development and documentation workflows.

## Installation

```
composer require flow-php/openapi-specification-bridge:~--FLOW_PHP_VERSION--
```

## Usage

This bridge allows you to convert Flow PHP schemas to OpenAPI specification format and vice versa.

### Converting Flow Schema to OpenAPI

```php
<?php

use function Flow\ETL\DSL\{schema, int_schema, str_schema, bool_schema};
use function Flow\Bridge\OpenAPI\Specification\DSL\schema_to_openapi_specification;
use Flow\Bridge\OpenAPI\Specification\OpenAPIMetadata;

$userSchema = schema(
    int_schema('id', false, 
        OpenAPIMetadata::description('Unique user identifier')
            ->merge(OpenAPIMetadata::example(123))
    ),
    str_schema('name', true, 
        OpenAPIMetadata::description('User full name')
            ->merge(OpenAPIMetadata::example('John Doe'))
    ),
    bool_schema('active', false, 
        OpenAPIMetadata::description('Account status')
    )
);

$openApiSpec = schema_to_openapi_specification($userSchema);

// Output:
// [
//     'type' => 'object',
//     'properties' => [
//         'id' => [
//             'type' => 'integer', 
//             'nullable' => false,
//             'description' => 'Unique user identifier',
//             'example' => 123
//         ],
//         'name' => [
//             'type' => 'string', 
//             'nullable' => true,
//             'description' => 'User full name', 
//             'example' => 'John Doe'
//         ],
//         'active' => [
//             'type' => 'boolean', 
//             'nullable' => false,
//             'description' => 'Account status'
//         ]
//     ]
// ]
```

### Converting OpenAPI to Flow Schema

```php
<?php

use function Flow\Bridge\OpenAPI\Specification\DSL\schema_from_openapi_specification;

$openApiSpec = [
    'type' => 'object',
    'properties' => [
        'id' => ['type' => 'integer', 'nullable' => false],
        'email' => [
            'type' => 'string', 
            'format' => 'email', 
            'nullable' => false,
            'description' => 'User email address'
        ],
        'profile' => [
            'type' => 'object',
            'properties' => [
                'name' => ['type' => 'string', 'nullable' => false],
                'bio' => ['type' => 'string', 'nullable' => true]
            ],
            'nullable' => false
        ],
        'tags' => [
            'type' => 'array',
            'items' => ['type' => 'string'],
            'nullable' => true
        ]
    ]
];

$flowSchema = schema_from_openapi_specification($openApiSpec);

// Now you can use $flowSchema in Flow ETL pipelines
```


## OpenAPI Metadata

The OpenAPI Specification Bridge provides specialized metadata handling through the `OpenAPIMetadata` enum, allowing you to add OpenAPI-specific properties to your Flow schema definitions. This metadata is used during schema conversion to generate rich OpenAPI specifications.

### Available OpenAPI Metadata

The `OpenAPIMetadata` enum provides the following metadata types:

- `DESCRIPTION` - Human-readable description of the property
- `FORMAT` - OpenAPI format specification (e.g., 'email', 'date-time', 'uuid')
- `EXAMPLE` - Example value for the property
- `EXAMPLES` - Multiple named examples
- `DEPRECATED` - Mark property as deprecated
- `TITLE` - Short title for the property
- `DEFAULT` - Default value for the property
- `READ_ONLY` - Mark property as read-only
- `WRITE_ONLY` - Mark property as write-only
- `NULLABLE` - Override schema nullability

### Using OpenAPI Metadata

```php
<?php

use function Flow\ETL\DSL\{schema, int_schema, str_schema};
use Flow\Bridge\OpenAPI\Specification\{OpenAPIConverter, OpenAPIMetadata};

$userSchema = schema(
    int_schema('id', false, 
        OpenAPIMetadata::description('Unique user identifier')
            ->merge(OpenAPIMetadata::format('int64'))
            ->merge(OpenAPIMetadata::example(12345))
            ->merge(OpenAPIMetadata::readOnly())
    ),
    str_schema('email', false,
        OpenAPIMetadata::description('User email address')
            ->merge(OpenAPIMetadata::format('email'))
            ->merge(OpenAPIMetadata::example('user@example.com'))
            ->merge(OpenAPIMetadata::title('Email Address'))
    ),
    str_schema('password', false,
        OpenAPIMetadata::description('User password')
            ->merge(OpenAPIMetadata::format('password'))
            ->merge(OpenAPIMetadata::writeOnly())
    ),
    str_schema('status', false,
        OpenAPIMetadata::description('Account status')
            ->merge(OpenAPIMetadata::default('active'))
            ->merge(OpenAPIMetadata::examples([
                'active' => 'active',
                'inactive' => 'inactive',
                'suspended' => 'suspended'
            ]))
    )
);

$converter = new OpenAPIConverter();
$openApiSpec = $converter->toOpenAPI($userSchema);

// Results in:
// [
//     'type' => 'object',
//     'properties' => [
//         'id' => [
//             'type' => 'integer',
//             'nullable' => false,
//             'description' => 'Unique user identifier',
//             'format' => 'int64',
//             'example' => 12345,
//             'readOnly' => true
//         ],
//         'email' => [
//             'type' => 'string',
//             'nullable' => false,
//             'description' => 'User email address',
//             'format' => 'email',
//             'example' => 'user@example.com',
//             'title' => 'Email Address'
//         ],
//         'password' => [
//             'type' => 'string',
//             'nullable' => false,
//             'description' => 'User password',
//             'format' => 'password',
//             'writeOnly' => true
//         ],
//         'status' => [
//             'type' => 'string',
//             'nullable' => false,
//             'description' => 'Account status',
//             'default' => 'active',
//             'examples' => [
//                 'active' => 'active',
//                 'inactive' => 'inactive',
//                 'suspended' => 'suspended'
//             ]
//         ]
//     ]
// ]
```

### Metadata Priority System

The OpenAPI bridge uses a priority system when handling metadata:

1. **OpenAPI-specific metadata** (from `OpenAPIMetadata`) takes highest priority
2. **Legacy metadata keys** are used as fallback when OpenAPI metadata is not present

```php
<?php

use Flow\ETL\Schema\Metadata;
use Flow\Bridge\OpenAPI\Specification\OpenAPIMetadata;

// OpenAPI metadata takes priority over legacy metadata
$metadata = Metadata::with('description', 'Legacy description')
    ->merge(OpenAPIMetadata::description('OpenAPI description')) // This wins
    ->merge(Metadata::with('example', 'Legacy example'))
    ->merge(OpenAPIMetadata::example('OpenAPI example')); // This wins

$schema = schema(
    str_schema('field', false, $metadata)
);

// Result will use OpenAPI values: "OpenAPI description" and "OpenAPI example"
```

### Common OpenAPI Formats

Here are some commonly used OpenAPI formats you can specify with `OpenAPIMetadata::format()`:

**String formats:**
- `email` - Email address
- `uri` - URI/URL
- `uuid` - UUID string
- `date` - Date (YYYY-MM-DD)
- `date-time` - Date and time (RFC 3339)
- `password` - Password field
- `byte` - Base64 encoded string
- `binary` - Binary data

**Integer formats:**
- `int32` - 32-bit integer
- `int64` - 64-bit integer

**Number formats:**
- `float` - Floating point number
- `double` - Double precision number

### Usage Example

```php
<?php

use function Flow\ETL\DSL\{schema, int_schema, str_schema, list_schema, structure_schema};
use function Flow\Types\DSL\{type_list, type_string, type_structure};
use function Flow\Bridge\OpenAPI\Specification\DSL\schema_to_openapi_specification;
use Flow\Bridge\OpenAPI\Specification\OpenAPIMetadata;

// Define your data schema with rich OpenAPI metadata
$productSchema = schema(
    int_schema('id', false, 
        OpenAPIMetadata::description('Product identifier')
            ->merge(OpenAPIMetadata::format('int64'))
            ->merge(OpenAPIMetadata::example(123))
            ->merge(OpenAPIMetadata::readOnly())
    ),
    str_schema('name', false,
        OpenAPIMetadata::description('Product name')
            ->merge(OpenAPIMetadata::title('Name'))
            ->merge(OpenAPIMetadata::example('iPhone 15'))
    ),
    str_schema('description', true,
        OpenAPIMetadata::description('Detailed product description')
            ->merge(OpenAPIMetadata::example('Latest smartphone with advanced features'))
    ),
    structure_schema('category', type_structure([
        'id' => type_string(),
        'name' => type_string()
    ]), false, 
        OpenAPIMetadata::description('Product category information')
    ),
    list_schema('tags', type_list(type_string()), true,
        OpenAPIMetadata::description('Product tags for categorization')
            ->merge(OpenAPIMetadata::examples([
                'electronics' => 'electronics',
                'smartphone' => 'smartphone',
                'apple' => 'apple'
            ]))
    )
);

$apiSpec = schema_to_openapi_specification($productSchema);

// Use in your API documentation
$fullApiSpec = [
    'openapi' => '3.0.0',
    'info' => [
        'title' => 'Product API',
        'version' => '1.0.0'
    ],
    'paths' => [
        '/products' => [
            'get' => [
                'summary' => 'List products',
                'responses' => [
                    '200' => [
                        'description' => 'A list of products',
                        'content' => [
                            'application/json' => [
                                'schema' => [
                                    'type' => 'array',
                                    'items' => $apiSpec
                                ]
                            ]
                        ]
                    ]
                ]
            ]
        ]
    ]
];
```