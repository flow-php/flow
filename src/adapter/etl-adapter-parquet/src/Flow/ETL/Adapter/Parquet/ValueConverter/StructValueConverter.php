<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\ValueConverter;

use function array_key_exists;
use function is_array;

final readonly class StructValueConverter implements ValueConverter
{
    /**
     * @param array<string, ValueConverter> $children
     */
    public function __construct(
        private array $children,
    ) {}

    public function decode(mixed $value): mixed
    {
        if (!is_array($value)) {
            return $value;
        }

        foreach ($this->children as $name => $converter) {
            if (array_key_exists($name, $value) && $value[$name] !== null) {
                $value[$name] = $converter->decode($value[$name]);
            }
        }

        return $value;
    }

    public function encode(mixed $value): mixed
    {
        if (!is_array($value)) {
            return $value;
        }

        foreach ($this->children as $name => $converter) {
            if (array_key_exists($name, $value) && $value[$name] !== null) {
                $value[$name] = $converter->encode($value[$name]);
            }
        }

        return $value;
    }
}
