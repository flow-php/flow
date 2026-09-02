<?php

declare(strict_types=1);

namespace Flow\ETL\Exception;

use Flow\ETL\Schema\Definition;
use Flow\Types\Type\TypeDetector;

use function get_debug_type;
use function is_bool;
use function is_float;
use function is_int;
use function is_string;
use function sprintf;
use function strlen;
use function substr;

final class ColumnMismatchException extends InvalidArgumentException
{
    private function __construct(
        public readonly string $column,
        public readonly string $detail,
    ) {
        parent::__construct('Row does not match its schema: column "' . $column . '"' . $detail);
    }

    /**
     * @param Definition<mixed> $definition
     */
    public static function missingColumn(Definition $definition): self
    {
        return new self($definition->entry()->name(), ' declared by the schema is missing from the row');
    }

    public static function unexpectedColumn(string $column): self
    {
        return new self($column, ' is not declared by the schema');
    }

    /**
     * @param Definition<mixed> $definition
     */
    public static function valueDoesNotMatch(Definition $definition, mixed $value): self
    {
        return new self(
            $definition->entry()->name(),
            $value === null
                ? sprintf(': could not convert null to %s, column is not nullable', $definition->type()->toString())
                : sprintf(
                    ': could not convert %s (%s) to %s',
                    self::describe($value),
                    (new TypeDetector())
                        ->detectType($value)
                        ->toString(),
                    $definition->type()->toString(),
                ),
        );
    }

    private static function describe(mixed $value): string
    {
        return match (true) {
            is_string($value) => "'" . (strlen($value) > 32 ? substr($value, 0, 32) . '...' : $value) . "'",
            is_bool($value) => $value ? 'true' : 'false',
            is_int($value), is_float($value) => (string) $value,
            default => get_debug_type($value),
        };
    }
}
