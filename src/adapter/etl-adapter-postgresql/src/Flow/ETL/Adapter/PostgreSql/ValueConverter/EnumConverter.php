<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\ValueConverter;

use Flow\PostgreSql\Client\Types\ValueConverter;
use Flow\PostgreSql\Client\Types\ValueType;

final readonly class EnumConverter implements ValueConverter
{
    public function __construct(
        private ValueConverter $next = new HTMLConverter(),
    ) {}

    public function supportedTypes(): array
    {
        return [ValueType::TEXT];
    }

    public function toDatabase(mixed $value): ?string
    {
        if ($value === null) {
            return null;
        }

        if ($value instanceof \BackedEnum) {
            return (string) $value->value;
        }

        if ($value instanceof \UnitEnum) {
            return $value->name;
        }

        return $this->next->toDatabase($value);
    }
}
