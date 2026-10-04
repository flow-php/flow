<?php

declare(strict_types=1);

namespace Flow\ETL\Exception;

use DateTimeInterface;
use Flow\ETL\Schema\Definition;
use Flow\Types\Exception\Exception as TypesException;
use Flow\Types\Type\TypeDetector;
use Throwable;

use function get_debug_type;
use function is_bool;
use function is_float;
use function is_infinite;
use function is_int;
use function is_nan;
use function is_string;
use function sprintf;
use function strlen;
use function substr;

final class ColumnMismatchException extends InvalidArgumentException
{
    private function __construct(
        public readonly string $column,
        public readonly string $detail,
        ?Throwable $previous = null,
    ) {
        parent::__construct('Row does not match its schema: column "' . $column . '"' . $detail, 0, $previous);
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
    public static function retype(Definition $definition, string $reason): self
    {
        return new self($definition->entry()->name(), sprintf(
            ': cannot retype the column to %s, %s',
            $definition->type()->toString(),
            $reason,
        ));
    }

    /**
     * @param Definition<mixed> $definition
     */
    public static function unsupportedType(Definition $definition, string $reason): self
    {
        return new self($definition->entry()->name(), sprintf(
            ': %s cannot be a batch column, %s',
            $definition->type()->toString(),
            $reason,
        ));
    }

    /**
     * @param Definition<mixed> $definition
     */
    public static function untypedColumn(Definition $definition): self
    {
        return new self(
            $definition->entry()->name(),
            ': mixed cannot be a batch column, an untyped function result exists only inside function evaluation',
        );
    }

    /**
     * @param Definition<mixed> $definition
     */
    public static function valueDoesNotMatch(Definition $definition, mixed $value, ?TypesException $reason = null): self
    {
        return new self(
            $definition->entry()->name(),
            (
                $value === null
                    ? sprintf(': could not convert null to %s, column is not nullable', $definition->type()->toString())
                    : sprintf(
                        ': could not convert %s (%s) to %s',
                        self::describe($value),
                        (new TypeDetector())
                            ->detectType($value)
                            ->toString(),
                        $definition->type()->toString(),
                    )
            ) . ($reason === null ? '' : ' - ' . $reason->getMessage()),
            $reason,
        );
    }

    private static function describe(mixed $value): string
    {
        return match (true) {
            is_string($value) => "'" . (strlen($value) > 32 ? substr($value, 0, 32) . '...' : $value) . "'",
            is_bool($value) => $value ? 'true' : 'false',
            is_float($value) && is_nan($value) => 'NAN',
            is_float($value) && is_infinite($value) => $value > 0 ? 'INF' : '-INF',
            is_int($value), is_float($value) => (string) $value,
            $value instanceof DateTimeInterface => $value->format('Y-m-d\TH:i:s.uP'),
            default => get_debug_type($value),
        };
    }
}
