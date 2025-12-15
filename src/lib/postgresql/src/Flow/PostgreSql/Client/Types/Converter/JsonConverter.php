<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use function Flow\Types\DSL\type_json;
use Flow\PostgreSql\Client\Types\{PostgreSqlType, ValueConverter};
use Flow\Types\Type;

use Flow\Types\Value\Json;

/**
 * @implements ValueConverter<Json>
 */
final class JsonConverter implements ValueConverter
{
    public function flowType() : Type
    {
        return type_json();
    }

    public function supportedTypes() : array
    {
        return [
            PostgreSqlType::JSON,
            PostgreSqlType::JSONB,
        ];
    }

    public function toDatabase(mixed $value) : ?string
    {
        if ($value === null) {
            return null;
        }

        if ($value instanceof Json) {
            return $value->toString();
        }

        if (\is_string($value)) {
            return $value;
        }

        return \json_encode($value, \JSON_THROW_ON_ERROR | \JSON_PRESERVE_ZERO_FRACTION);
    }

    public function toPhp(string $value, PostgreSqlType $type) : Json
    {
        return Json::fromString($value);
    }
}
