<?php

declare(strict_types=1);

namespace Flow\Bridge\OpenAPI\Specification;

use Flow\ETL\Schema\Metadata;

enum OpenAPIMetadata : string
{
    case DEFAULT = 'openapi_default';
    case DEPRECATED = 'openapi_deprecated';
    case DESCRIPTION = 'openapi_description';
    case EXAMPLE = 'openapi_example';
    case EXAMPLES = 'openapi_examples';
    case FORMAT = 'openapi_format';
    case NULLABLE = 'openapi_nullable';
    case READ_ONLY = 'openapi_read_only';
    case TITLE = 'openapi_title';
    case WRITE_ONLY = 'openapi_write_only';

    /**
     * @param array<mixed>|bool|float|int|string $default
     */
    public static function default($default) : Metadata
    {
        return Metadata::with(self::DEFAULT->value, $default);
    }

    public static function deprecated(bool $deprecated = true) : Metadata
    {
        return Metadata::with(self::DEPRECATED->value, $deprecated);
    }

    public static function description(string $description) : Metadata
    {
        return Metadata::with(self::DESCRIPTION->value, $description);
    }

    /**
     * @param array<mixed>|bool|float|int|string $example
     */
    public static function example($example) : Metadata
    {
        return Metadata::with(self::EXAMPLE->value, $example);
    }

    /**
     * @param array<string, mixed> $examples
     */
    public static function examples(array $examples) : Metadata
    {
        return Metadata::with(self::EXAMPLES->value, $examples);
    }

    public static function format(string $format) : Metadata
    {
        return Metadata::with(self::FORMAT->value, $format);
    }

    public static function nullable(bool $nullable = true) : Metadata
    {
        return Metadata::with(self::NULLABLE->value, $nullable);
    }

    public static function readOnly(bool $readOnly = true) : Metadata
    {
        return Metadata::with(self::READ_ONLY->value, $readOnly);
    }

    public static function title(string $title) : Metadata
    {
        return Metadata::with(self::TITLE->value, $title);
    }

    public static function writeOnly(bool $writeOnly = true) : Metadata
    {
        return Metadata::with(self::WRITE_ONLY->value, $writeOnly);
    }
}
