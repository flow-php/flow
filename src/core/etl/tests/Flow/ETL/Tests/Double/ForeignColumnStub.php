<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Column\Column;
use Flow\Types\Type;

/**
 * A Column no backend owns: every call reaches the wrapped column.
 */
final readonly class ForeignColumnStub implements Column
{
    public function __construct(
        private Column $column,
    ) {}

    public function at(int $i): mixed
    {
        return $this->column->at($i);
    }

    public function concat(Column ...$others): Column
    {
        return $this->column->concat(...$others);
    }

    public function count(): int
    {
        return $this->column->count();
    }

    public function encode(): array
    {
        return $this->column->encode();
    }

    public function isNull(int $i): bool
    {
        return $this->column->isNull($i);
    }

    public function nullCount(): int
    {
        return $this->column->nullCount();
    }

    public function physicals(): array
    {
        return $this->column->physicals();
    }

    public function slice(int $offset, int $length): Column
    {
        return new self($this->column->slice($offset, $length));
    }

    public function take(array $indices): Column
    {
        return $this->column->take($indices);
    }

    public function type(): Type
    {
        return $this->column->type();
    }

    public function value(int $i): mixed
    {
        return $this->column->value($i);
    }

    public function values(): array
    {
        return $this->column->values();
    }

    public function withType(Type $type): Column
    {
        return $this->column->withType($type);
    }
}
