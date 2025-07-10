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
use Flow\ETL\Schema\Metadata;

$userSchema = schema(
    int_schema('id', false, Metadata::empty()
        ->add('description', 'Unique user identifier')
        ->add('example', 123)
    ),
    str_schema('name', true, Metadata::empty()
        ->add('description', 'User full name')
        ->add('example', 'John Doe')
    ),
    bool_schema('active', false, Metadata::empty()
        ->add('description', 'Account status')
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


### Usage Example

```php
<?php

use function Flow\ETL\DSL\{schema, int_schema, str_schema, list_schema, structure_schema};
use function Flow\Types\DSL\{type_list, type_string, type_structure};
use function Flow\Bridge\OpenAPI\Specification\DSL\schema_to_openapi_specification;

// Define your data schema
$productSchema = schema(
    int_schema('id', false),
    str_schema('name', false),
    str_schema('description', true),
    structure_schema('category', type_structure([
        'id' => type_string(),
        'name' => type_string()
    ]), false),
    list_schema('tags', type_list(type_string()), true)
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

## Supported OpenAPI Features

### ✅ Supported
- Basic types (boolean, integer, number, string)
- String formats (date, date-time, time, uuid, json, xml)
- Arrays with typed items
- Objects with properties and additionalProperties
- Nullable types
- Descriptions and examples
- Nested structures