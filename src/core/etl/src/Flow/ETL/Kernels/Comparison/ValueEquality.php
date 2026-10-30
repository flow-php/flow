<?php

declare(strict_types=1);

namespace Flow\ETL\Kernels\Comparison;

use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Column\ComparableValues;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Schema\Definitions;
use Flow\Types\Exception\CastingException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\StringType;
use Flow\Types\Type\TypeDetector;

use function array_combine;
use function array_keys;
use function array_unique;
use function array_values;
use function filter_var;
use function Flow\Types\DSL\type_bare;
use function get_debug_type;
use function is_array;
use function is_numeric;
use function is_string;
use function round;
use function sprintf;
use function trim;
use function var_export;

use const FILTER_VALIDATE_INT;

/**
 * SQL equality: two columns of different types compare in one type, as DuckDB binds them. Numbers widen to float, a
 * date to a datetime, and a string casts to the other side's type; a string that does not cast throws.
 */
final readonly class ValueEquality
{
    /**
     * $column's values in $type, built on PhpBackend: a cast column never leaves this class and its physicals go
     * straight back to PHP, so a native build would only add a copy.
     *
     * @param Type<mixed> $type
     */
    public function cast(Column $column, Type $type, string $name): Column
    {
        if (type_bare($column->type())::class === type_bare($type)::class) {
            return $column;
        }

        $target = type_bare($type);
        $values = [];
        $i = 0;

        try {
            // @mago-ignore analysis:mixed-assignment
            foreach ($column->values() as $i => $value) {
                $values[] = match (true) {
                    $value === null => null,
                    is_string($value) && ($target instanceof IntegerType || $target instanceof FloatType)
                        => $this->number($value, $target),
                    default => $type->cast($value),
                };
            }
        } catch (CastingException|InvalidArgumentException $e) {
            throw EvaluationException::at(
                $i,
                new InvalidArgumentException(
                    sprintf(
                        'Column "%s" value %s cannot be cast to %s to compare: %s',
                        $name,
                        var_export($column->value($i), true),
                        $target->toString(),
                        $e->getMessage(),
                    ),
                    0,
                    $e,
                ),
            );
        }

        $builder = (new PhpBackend())->builder(Definitions::of($name, $target, $column->nullCount() > 0));
        $builder->appendMany($values);

        return $builder->finish();
    }

    /**
     * @return list<?bool> null where either value is null
     */
    public function equal(Column $left, Column $right, string $leftName = 'left', string $rightName = 'right'): array
    {
        $type = $this->type($left->type(), $right->type());
        $comparable = new ComparableValues();
        $lefts = $comparable->equality($this->cast($left, $type, $leftName));
        $rights = $comparable->equality($this->cast($right, $type, $rightName));
        $results = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($lefts as $i => $leftValue) {
            $results[] = $leftValue === null || $rights[$i] === null ? null : $leftValue === $rights[$i];
        }

        return $results;
    }

    /**
     * SQL IN, an OR of equal() over each row's candidates: a candidate that does not cast to the type it compares
     * in with its needle throws, even beside a match.
     *
     * @param list<?array<array-key, mixed>> $candidates one haystack per needle row, null for a null haystack
     *
     * @return list<?bool> null for a null needle or haystack, and for no match beside a null candidate
     */
    public function in(Column $needles, array $candidates, string $needleName, string $haystackName): array
    {
        $detector = new TypeDetector();
        $groups = [];
        $results = [];

        foreach ($candidates as $row => $haystack) {
            if ($haystack === null || $needles->isNull($row)) {
                $results[] = null;

                continue;
            }

            $results[] = false;

            // @mago-ignore analysis:mixed-assignment
            foreach ($haystack as $candidate) {
                if ($candidate === null) {
                    $results[$row] = null;

                    continue;
                }

                $key = is_array($candidate)
                    ? $detector->detectType($candidate)->toString()
                    : get_debug_type($candidate);
                $groups[$key] ??= ['type' => $detector->detectType($candidate), 'values' => [], 'rows' => []];
                $groups[$key]['values'][] = $candidate;
                $groups[$key]['rows'][] = $row;
            }
        }

        $comparable = new ComparableValues();
        $matched = [];

        foreach ($groups as ['type' => $candidateType, 'values' => $values, 'rows' => $rows]) {
            $type = $this->type($needles->type(), $candidateType);
            $needleRows = array_values(array_unique($rows));
            $builder = (new PhpBackend())->builder(Definitions::of($haystackName, $candidateType, false));
            $builder->appendMany($values);

            try {
                $needleValues = array_combine(
                    $needleRows,
                    $comparable->equality($this->cast($needles->take($needleRows), $type, $needleName)),
                );
            } catch (EvaluationException $e) {
                throw EvaluationException::at($needleRows[$e->rowIndex], $e);
            }

            try {
                $candidateValues = $comparable->equality($this->cast($builder->finish(), $type, $haystackName));
            } catch (EvaluationException $e) {
                throw EvaluationException::at($rows[$e->rowIndex], $e);
            }

            // @mago-ignore analysis:mixed-assignment
            foreach ($candidateValues as $i => $candidateValue) {
                if ($candidateValue === $needleValues[$rows[$i]]) {
                    $matched[$rows[$i]] = true;
                }
            }
        }

        foreach (array_keys($matched) as $row) {
            $results[$row] = true;
        }

        return $results;
    }

    /**
     * PHP identity on the comparable form, never across types: null === null.
     *
     * @return list<bool>
     */
    public function identical(Column $left, Column $right): array
    {
        $comparable = new ComparableValues();
        [$lefts, $rights] = type_bare($left->type())::class === type_bare($right->type())::class
            ? [$comparable->equality($left), $comparable->equality($right)]
            : [$left->values(), $right->values()];
        $results = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($lefts as $i => $leftValue) {
            $results[] = $leftValue === $rights[$i];
        }

        return $results;
    }

    /**
     * The one type a pair compares in.
     *
     * @param Type<mixed> $left
     * @param Type<mixed> $right
     *
     * @return Type<mixed>
     */
    public function type(Type $left, Type $right): Type
    {
        $leftBare = type_bare($left);
        $rightBare = type_bare($right);

        return match (true) {
            $leftBare::class === $rightBare::class => $leftBare,
            $leftBare instanceof IntegerType && $rightBare instanceof FloatType,
            $leftBare instanceof DateType && $rightBare instanceof DateTimeType,
            $leftBare instanceof StringType,
                => $rightBare,
            default => $leftBare,
        };
    }

    /**
     * A numeric string as DuckDB casts it: an integer target rounds half away from zero.
     */
    public function number(string $value, IntegerType|FloatType $type): int|float
    {
        if (!is_numeric($value)) {
            throw new InvalidArgumentException(sprintf('"%s" is not a number', $value));
        }

        if ($type instanceof FloatType) {
            return (float) $value;
        }

        // an integral string stays exact past 2^53, where a float would round it
        $integer = filter_var(trim($value), FILTER_VALIDATE_INT);

        return $integer === false ? (int) round((float) $value) : $integer;
    }
}
