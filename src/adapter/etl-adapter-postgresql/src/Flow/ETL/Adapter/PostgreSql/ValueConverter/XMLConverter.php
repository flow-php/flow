<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\ValueConverter;

use Flow\PostgreSql\Client\Types\Converter\StringConverter;
use Flow\PostgreSql\Client\Types\{PostgreSqlType, ValueConverter};

final readonly class XMLConverter implements ValueConverter
{
    public function __construct(
        private StringConverter $stringConverter = new StringConverter(),
    ) {
    }

    public function supportedTypes() : array
    {
        return [PostgreSqlType::XML];
    }

    public function toDatabase(mixed $value) : ?string
    {
        if ($value === null) {
            return null;
        }

        if ($value instanceof \DOMDocument) {
            return $value->saveXML() ?: null;
        }

        if ($value instanceof \DOMElement) {
            return $value->ownerDocument?->saveXML($value) ?: null;
        }

        return $this->stringConverter->toDatabase($value);
    }
}
