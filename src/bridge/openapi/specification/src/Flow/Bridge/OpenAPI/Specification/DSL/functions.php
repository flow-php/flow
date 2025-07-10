<?php

declare(strict_types=1);

namespace Flow\Bridge\OpenAPI\Specification\DSL;

use Flow\Bridge\OpenAPI\Specification\OpenAPIConverter;
use Flow\ETL\Schema;

/**
 * Convert Flow Schema to OpenAPI specification format.
 *
 * @return array<string, mixed>
 */
function to_openapi_spec(Schema $schema) : array
{
    return (new OpenAPIConverter())->toOpenAPI($schema);
}

/**
 * Convert OpenAPI specification to Flow Schema.
 *
 * @param array<string, mixed> $openApiSpec
 */
function from_openapi_spec(array $openApiSpec) : Schema
{
    return (new OpenAPIConverter())->fromOpenAPI($openApiSpec);
}
