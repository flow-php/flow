<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Explain;

use DateTimeInterface;
use Flow\ETL\Function\All;
use Flow\ETL\Function\Any;
use Flow\ETL\Function\Between;
use Flow\ETL\Function\Between\Boundary;
use Flow\ETL\Function\Equals;
use Flow\ETL\Function\FunctionTree;
use Flow\ETL\Function\GreaterThan;
use Flow\ETL\Function\GreaterThanEqual;
use Flow\ETL\Function\IsIn;
use Flow\ETL\Function\IsNotNull;
use Flow\ETL\Function\IsNull;
use Flow\ETL\Function\LessThan;
use Flow\ETL\Function\LessThanEqual;
use Flow\ETL\Function\Literal;
use Flow\ETL\Function\Not;
use Flow\ETL\Function\NotEquals;
use Flow\ETL\Function\NotSame;
use Flow\ETL\Function\Same;
use Flow\ETL\Function\StartsWith;
use Flow\ETL\Row\Reference;
use ReflectionClass;
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
            $function instanceof Equals => $this->pair($children, '='),
            $function instanceof NotEquals => $this->pair($children, '!='),
            $function instanceof Same => $this->pair($children, '==='),
            $function instanceof NotSame => $this->pair($children, '!=='),
            $function instanceof GreaterThan => $this->pair($children, '>'),
            $function instanceof GreaterThanEqual => $this->pair($children, '>='),
            $function instanceof LessThan => $this->pair($children, '<'),
            $function instanceof LessThanEqual => $this->pair($children, '<='),
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
            (new ReflectionClass($function))->getShortName()
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
            $value instanceof UnitEnum => (new ReflectionClass($value))->getShortName() . '::' . $value->name,
            is_object($value) => (new ReflectionClass($value))->getShortName(),
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
