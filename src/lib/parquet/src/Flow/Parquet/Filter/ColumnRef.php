<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter;

use Flow\Parquet\Exception\InvalidArgumentException;

final readonly class ColumnRef
{
    public function __construct(
        public string $path,
    ) {
        if ($path === '') {
            throw new InvalidArgumentException('A column path cannot be empty');
        }
    }

    public function equals(mixed $value): Comparison
    {
        return new Comparison($this, Operator::Equal, $value);
    }

    public function greaterThan(mixed $value): Comparison
    {
        return new Comparison($this, Operator::GreaterThan, $value);
    }

    public function greaterThanOrEqual(mixed $value): Comparison
    {
        return new Comparison($this, Operator::GreaterThanOrEqual, $value);
    }

    /**
     * @param array<mixed> $values
     */
    public function in(array $values): In
    {
        return new In($this, $values);
    }

    public function isNotNull(): Not
    {
        return new Not($this->isNull());
    }

    public function isNull(): IsNull
    {
        return new IsNull($this);
    }

    public function lessThan(mixed $value): Comparison
    {
        return new Comparison($this, Operator::LessThan, $value);
    }

    public function lessThanOrEqual(mixed $value): Comparison
    {
        return new Comparison($this, Operator::LessThanOrEqual, $value);
    }

    public function notEquals(mixed $value): Not
    {
        return new Not($this->equals($value));
    }

    /**
     * @param array<mixed> $values
     */
    public function notIn(array $values): Not
    {
        return new Not($this->in($values));
    }

    public function startsWith(string $prefix): StartsWith
    {
        return new StartsWith($this, $prefix);
    }
}
