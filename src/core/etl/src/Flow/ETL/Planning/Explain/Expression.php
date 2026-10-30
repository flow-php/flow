<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Explain;

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
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Functions\StartsWith;
use Flow\Telemetry\ObjectExtractor;
use Flow\Types\Type\Comparison\Operator;
use UnitEnum;

use function addcslashes;
use function array_map;
use function array_slice;
use function count;
use function Flow\Types\DSL\type_instance_of;
use function implode;
use function is_array;
use function is_bool;
use function is_float;
use function is_object;
use function is_string;
use function str_replace;
use function var_export;

final readonly class Expression
{
    private const int LIST_LIMIT = 5;

    public function of(FunctionTree $function): string
    {
        return match (true) {
            $function instanceof Reference => $function->name(),
            $function instanceof Literal => $this->value($function->value()),
            default => $this->operator($function) ?? $this->call($function),
        };
    }

    /**
     * An operator inside another operator is parenthesized; a reference, a value or a call reads unambiguously as is.
     */
    public function operand(FunctionTree $function): string
    {
        $operator = $this->operator($function);

        return $operator === null ? $this->of($function) : '(' . $operator . ')';
    }

    /**
     * Null for a function the explain has no wording for.
     */
    public function operator(FunctionTree $function): ?string
    {
        $children = $function->children();

        return match (true) {
            $function instanceof All => $this->composite($children, ' AND '),
            $function instanceof Any => $this->composite($children, ' OR '),
            $function instanceof Not => 'NOT ' . $this->operand($children[0]),
            $function instanceof Comparison => $this->pair(
                $children,
                $function->operator === Operator::EQUAL ? '=' : $function->operator->value,
            ),
            $function instanceof IsIn => $this->operand($children[1]) . ' IN ' . $this->operand($children[0]),
            $function instanceof StartsWith => $this->pair($children, 'STARTS WITH'),
            $function instanceof IsNull => $this->operand($children[0]) . ' IS NULL',
            $function instanceof IsNotNull => $this->operand($children[0]) . ' IS NOT NULL',
            $function instanceof Between => $this->between($children),
            default => null,
        };
    }

    /**
     * Comparisons bind tighter than AND/OR, so only a nested AND/OR is parenthesized.
     *
     * @param list<FunctionTree> $children
     */
    public function composite(array $children, string $operator): string
    {
        return implode($operator, array_map(fn(FunctionTree $each): string => $each instanceof All
            || $each instanceof Any
                ? '(' . $this->of($each) . ')'
                : $this->of($each), $children));
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function pair(array $children, string $operator): string
    {
        return $this->operand($children[0]) . ' ' . $operator . ' ' . $this->operand($children[1]);
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function between(array $children): ?string
    {
        $boundary = $children[3];
        $type = type_instance_of(Boundary::class);

        if (!$boundary instanceof Literal || !$type->isValid($boundary->value())) {
            return null;
        }

        return $this->range(
            $type->assert($boundary->value()),
            $this->operand($children[0]),
            $this->operand($children[1]),
            $this->operand($children[2]),
        );
    }

    public function range(Boundary $boundary, string $value, string $lower, string $upper): string
    {
        return match ($boundary) {
            Boundary::INCLUSIVE => $value . ' BETWEEN ' . $lower . ' AND ' . $upper,
            Boundary::LEFT_INCLUSIVE => $lower . ' <= ' . $value . ' < ' . $upper,
            Boundary::RIGHT_INCLUSIVE => $lower . ' < ' . $value . ' <= ' . $upper,
            Boundary::EXCLUSIVE => $lower . ' < ' . $value . ' < ' . $upper,
        };
    }

    public function call(FunctionTree $function): string
    {
        return (
            ObjectExtractor::shortName($function)
            . '('
            . implode(', ', array_map($this->of(...), $function->children()))
            . ')'
        );
    }

    public function value(mixed $value): string
    {
        return match (true) {
            $value === null => 'null',
            is_bool($value) => $value ? 'true' : 'false',
            is_string($value) => "'" . addcslashes(str_replace("'", "''", $value), "\0..\37") . "'",
            is_float($value) => var_export($value, true),
            is_array($value) => $this->list($value),
            $value instanceof DateTimeInterface => $value->format(DateTimeInterface::ATOM),
            $value instanceof UnitEnum => ObjectExtractor::shortName($value) . '::' . $value->name,
            is_object($value) => ObjectExtractor::shortName($value),
            default => (string) $value,
        };
    }

    /**
     * Cut after a few values, so a long list never makes an unbounded line.
     *
     * @param array<array-key, mixed> $values
     */
    public function list(array $values): string
    {
        $shown = implode(', ', array_map($this->value(...), array_slice($values, 0, self::LIST_LIMIT)));
        $rest = count($values) - self::LIST_LIMIT;

        return $rest > 0 ? '(' . $shown . ', … ' . $rest . ' more)' : '(' . $shown . ')';
    }
}
