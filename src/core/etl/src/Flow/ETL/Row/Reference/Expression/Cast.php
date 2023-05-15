<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class Cast implements Expression
{
    public function __construct(
        private readonly Expression $ref,
        private readonly string $type
    ) {
    }

    /**
     * @throws InvalidArgumentException
     * @throws \JsonException
     */
    public function eval(Row $row) : mixed
    {
        /** @psalm-suppress MixedAssignment */
        $value = $this->ref->eval($row);

        return match (\mb_strtolower($this->type)) {
            'datetime' => match (\gettype($value)) {
                'string' => new \DateTimeImmutable($value),
                'integer' => \DateTimeImmutable::createFromFormat('U', (string) $value),
                default => null,
            },
            'date' => match (\gettype($value)) {
                'string' => (new \DateTimeImmutable($value))->setTime(0, 0, 0, 0),
                'integer' => \DateTimeImmutable::createFromFormat('U', (string) $value),
                default => null,
            },
            /** @phpstan-ignore-next-line */
            'int', 'integer' => (int) $value,
            /** @phpstan-ignore-next-line */
            'float', 'double', 'real' => (float) $value,
            'string' => match (\gettype($value)) {
                'object', 'array' => \json_encode($value, JSON_THROW_ON_ERROR),
                /** @phpstan-ignore-next-line */
                default => (string) $value
            },
            'bool', 'boolean' => (bool) $value,
            'array' => (array) $value,
            'object' => (object) $value,
            'null' => null,
            'json' => \json_encode($value, JSON_THROW_ON_ERROR),
            'json_pretty' => \json_encode($value, JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT),
            default => throw new InvalidArgumentException("Unknown cast type '{$this->type}'")
        };
    }
}
