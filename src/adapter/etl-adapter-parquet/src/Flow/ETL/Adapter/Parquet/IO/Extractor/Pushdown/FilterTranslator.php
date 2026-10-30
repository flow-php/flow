<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\IO\Extractor\Pushdown;

use DateTimeInterface;
use Flow\ETL\Functions\All;
use Flow\ETL\Functions\Any;
use Flow\ETL\Functions\Between;
use Flow\ETL\Functions\Between\Boundary;
use Flow\ETL\Functions\Comparison;
use Flow\ETL\Functions\FunctionTree;
use Flow\ETL\Functions\IsIn;
use Flow\ETL\Functions\IsNotNull;
use Flow\ETL\Functions\IsNull;
use Flow\ETL\Functions\Literal;
use Flow\ETL\Functions\Not;
use Flow\ETL\Functions\Reference\UnresolvedReference;
use Flow\ETL\Functions\ReferencedColumns;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Functions\StartsWith;
use Flow\ETL\Values\Column\ComparableValues;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition;
use Flow\Parquet\Filter\All as ParquetAll;
use Flow\Parquet\Filter\Any as ParquetAny;
use Flow\Parquet\Filter\Binding\LiteralDomain;
use Flow\Parquet\Filter\Comparison as ParquetComparison;
use Flow\Parquet\Filter\Filter;
use Flow\Parquet\Filter\Operator;
use Flow\Types\Type;
use Flow\Types\Type\Comparison\Operator as ComparisonOperator;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\NumericStringType;
use Flow\Types\Type\Logical\UuidType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\StringType;
use Flow\Types\Type\TypeDetector;
use Flow\Types\Value\Uuid;

use function abs;
use function array_filter;
use function array_map;
use function array_values;
use function ctype_digit;
use function floor;
use function Flow\Parquet\DSL\parquet_column;
use function Flow\Types\DSL\type_bare;
use function in_array;
use function is_array;
use function is_bool;
use function is_float;
use function is_int;
use function is_nan;
use function is_string;

/**
 * A conjunct is pushed only when the parquet filter keeps every row the Flow predicate keeps; anything it cannot
 * prove that for is left to FileReadLoop.
 */
final readonly class FilterTranslator
{
    /**
     * @param list<ScalarFunction> $conjuncts
     *
     * @return list<TranslatedConjunct> one per translatable conjunct
     */
    public function translate(array $conjuncts, Schema $body): array
    {
        $translated = [];

        foreach ($conjuncts as $conjunct) {
            $parquet = $this->filter($conjunct, $body);

            if ($parquet !== null) {
                $translated[] = new TranslatedConjunct(
                    $parquet,
                    array_values((new ReferencedColumns())->in($conjunct)->names()),
                );
            }
        }

        return $translated;
    }

    private function filter(ScalarFunction $function, Schema $body): ?Filter
    {
        $children = $function->children();

        return match (true) {
            $function instanceof All, $function instanceof Any => $this->combined($function, $body),
            $function instanceof Not => $children[0] instanceof ScalarFunction
                ? $this->filter($children[0], $body)?->negate()
                : null,
            $function instanceof Comparison => $this->compared($function, $body),
            $function instanceof IsNull => $this->isNull($children[0], $body),
            $function instanceof IsNotNull => $this->isNull($children[0], $body)?->negate(),
            $function instanceof IsIn => $this->in($children[0], $children[1], $body),
            $function instanceof StartsWith => $this->startsWith($children[0], $children[1], $body),
            $function instanceof Between => $this->between($function, $body),
            default => null,
        };
    }

    private function compared(Comparison $function, Schema $body): ?Filter
    {
        $children = $function->children();
        $filter = $this->comparison(
            $children[0],
            $children[1],
            match ($function->operator) {
                ComparisonOperator::LESS_THAN => Operator::LessThan,
                ComparisonOperator::LESS_THAN_EQUAL => Operator::LessThanOrEqual,
                ComparisonOperator::GREATER_THAN => Operator::GreaterThan,
                ComparisonOperator::GREATER_THAN_EQUAL => Operator::GreaterThanOrEqual,
                default => Operator::Equal,
            },
            $body,
            $function->operator === ComparisonOperator::IDENTICAL
            || $function->operator === ComparisonOperator::NOT_IDENTICAL,
        );

        return $function->operator === ComparisonOperator::NOT_EQUAL
        || $function->operator === ComparisonOperator::NOT_IDENTICAL
            ? $filter?->negate()
            : $filter;
    }

    private function combined(All|Any $function, Schema $body): ?Filter
    {
        $filters = [];

        foreach ($function->children() as $child) {
            $filter = $this->filter($child, $body);

            if ($filter === null) {
                return null;
            }

            $filters[] = $filter;
        }

        return $function instanceof All ? new ParquetAll(...$filters) : new ParquetAny(...$filters);
    }

    private function comparison(
        FunctionTree $left,
        FunctionTree $right,
        Operator $operator,
        Schema $body,
        bool $strict,
    ): ?ParquetComparison {
        if ($left instanceof Literal && $right instanceof UnresolvedReference) {
            return $this->comparison(
                $right,
                $left,
                match ($operator) {
                    Operator::LessThan => Operator::GreaterThan,
                    Operator::LessThanOrEqual => Operator::GreaterThanOrEqual,
                    Operator::GreaterThan => Operator::LessThan,
                    Operator::GreaterThanOrEqual => Operator::LessThanOrEqual,
                    Operator::Equal => Operator::Equal,
                },
                $body,
                $strict,
            );
        }

        $definition = $this->definition($left, $body);

        if ($definition === null || !$right instanceof Literal) {
            return null;
        }

        $value = $this->literal(
            type_bare($definition->type()),
            $right->returns(),
            $right->value(),
            $operator !== Operator::Equal,
            $strict,
        );

        return $value === null ? null : new ParquetComparison(parquet_column($definition->name()), $operator, $value);
    }

    private function isNull(FunctionTree $reference, Schema $body): ?Filter
    {
        $definition = $this->definition($reference, $body);

        return $definition === null ? null : parquet_column($definition->name())->isNull();
    }

    private function in(FunctionTree $haystack, FunctionTree $needle, Schema $body): ?Filter
    {
        $definition = $this->definition($needle, $body);
        // @mago-expect analysis:mixed-assignment
        $list = $haystack instanceof Literal ? $haystack->value() : null;

        if ($definition === null || !is_array($list)) {
            return null;
        }

        // NULL never matches: x IN (1, NULL) keeps what x IN (1) keeps, and NOT (x IN (1, NULL)) is never TRUE
        $present = array_values(array_filter($list, static fn(mixed $element): bool => $element !== null));
        $detector = new TypeDetector();
        $values = array_map(fn(mixed $element): mixed => $this->literal(
            type_bare($definition->type()),
            $detector->detectType($element),
            $element,
            false,
            false,
        ), $present);

        return in_array(null, $values, true) ? null : parquet_column($definition->name())->in($values);
    }

    private function startsWith(FunctionTree $haystack, FunctionTree $needle, Schema $body): ?Filter
    {
        $definition = $this->definition($haystack, $body);
        // @mago-expect analysis:mixed-assignment
        $prefix = $needle instanceof Literal ? $needle->value() : null;

        return $definition !== null && type_bare($definition->type()) instanceof StringType && is_string($prefix)
            ? parquet_column($definition->name())->startsWith($prefix)
            : null;
    }

    private function between(Between $between, Schema $body): ?Filter
    {
        [$value, $lower, $upper, $boundary] = $between->children();

        // @mago-expect analysis:mixed-assignment
        $kind = $boundary instanceof Literal ? $boundary->value() : null;

        if (!$kind instanceof Boundary) {
            return null;
        }

        $lowerBound = $this->comparison(
            $value,
            $lower,
            $kind === Boundary::INCLUSIVE || $kind === Boundary::LEFT_INCLUSIVE
                ? Operator::GreaterThanOrEqual
                : Operator::GreaterThan,
            $body,
            false,
        );
        $upperBound = $this->comparison(
            $value,
            $upper,
            $kind === Boundary::INCLUSIVE || $kind === Boundary::RIGHT_INCLUSIVE
                ? Operator::LessThanOrEqual
                : Operator::LessThan,
            $body,
            false,
        );

        return $lowerBound === null || $upperBound === null ? null : $lowerBound->and($upperBound);
    }

    /**
     * @return null|Definition<mixed> the body column $function is a reference to, null when it is none
     */
    private function definition(FunctionTree $function, Schema $body): ?Definition
    {
        return $function instanceof UnresolvedReference ? $body->findDefinition($function->to()) : null;
    }

    /**
     * @param Type<mixed> $column
     * @param Type<mixed> $literal
     *
     * @return null|bool|DateTimeInterface|float|int|string the literal as the parquet filter takes it, null when
     *                                                         the pair cannot be pushed
     */
    private function literal(Type $column, Type $literal, mixed $value, bool $ordering, bool $strict): mixed
    {
        $literal = type_bare($literal);

        if (is_float($value) && is_nan($value)) {
            return null;
        }

        return match (true) {
            $column instanceof JsonType => null,
            $column instanceof NumericStringType => !$strict
                && $literal instanceof StringType
                && is_string($value)
                && ctype_digit($value)
                && ($value === '0' || $value[0] !== '0')
                    ? $value
                    : null,
            !$strict && $column instanceof FloatType && $literal instanceof IntegerType => is_int($value)
                && abs($value) <= LiteralDomain::EXACT_FLOAT_INTEGER
                    ? (float) $value
                    : null,
            !$strict && $column instanceof IntegerType && $literal instanceof FloatType => is_float($value)
                && floor($value) === $value
                && abs($value) < LiteralDomain::EXACT_FLOAT_INTEGER
                    ? (int) $value
                    : null,
            !$strict
                && (
                    $column instanceof DateTimeType
                    && $literal instanceof DateType
                    || $column instanceof DateType
                    && $literal instanceof DateTimeType
                )
                => $value instanceof DateTimeInterface ? $value : null,
            $column::class !== $literal::class,
            $ordering && $column instanceof UuidType,
            !(new ComparableValues())->equalByPhysical($column),
                => null,
            $value instanceof Uuid => $value->toString(),
            is_bool($value),
            is_int($value),
            is_float($value),
            is_string($value),
            $value instanceof DateTimeInterface,
                => $value,
            default => null,
        };
    }
}
