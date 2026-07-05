<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\JsonSchema;

use function strlen;
use function substr;

enum JsonSchemaMetadata: string
{
    case ANY = 'json_schema.any';
    case DEFAULT = 'json_schema.default';
    case DESCRIPTION = 'json_schema.description';
    case ENUM = 'json_schema.enum';
    case EXAMPLES = 'json_schema.examples';
    case EXCLUSIVE_MAXIMUM = 'json_schema.exclusiveMaximum';
    case EXCLUSIVE_MINIMUM = 'json_schema.exclusiveMinimum';
    case FORMAT = 'json_schema.format';
    case MAXIMUM = 'json_schema.maximum';
    case MAX_ITEMS = 'json_schema.maxItems';
    case MAX_LENGTH = 'json_schema.maxLength';
    case MINIMUM = 'json_schema.minimum';
    case MIN_ITEMS = 'json_schema.minItems';
    case MIN_LENGTH = 'json_schema.minLength';
    case PATTERN = 'json_schema.pattern';
    case PREFIX_ITEMS = 'json_schema.prefixItems';
    case TITLE = 'json_schema.title';

    public function keyword(): string
    {
        return substr($this->value, strlen('json_schema.'));
    }
}
