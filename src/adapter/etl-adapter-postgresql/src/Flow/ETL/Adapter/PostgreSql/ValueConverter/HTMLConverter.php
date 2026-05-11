<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\ValueConverter;

use Dom\HTMLDocument;
use Dom\HTMLElement;
use Flow\PostgreSql\Client\Types\Converter\StringConverter;
use Flow\PostgreSql\Client\Types\ValueConverter;
use Flow\PostgreSql\Client\Types\ValueType;

final readonly class HTMLConverter implements ValueConverter
{
    public function __construct(
        private ValueConverter $next = new StringConverter(),
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

        if ($value instanceof HTMLDocument) {
            return $value->saveHTML() ?: null;
        }

        if ($value instanceof HTMLElement) {
            return $value->ownerDocument->saveHTML($value) ?: null;
        }

        return $this->next->toDatabase($value);
    }
}
