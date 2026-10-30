<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Column\ColumnBuilder;
use Flow\ETL\Values\Column\Php\ConstantColumn;
use Flow\ETL\Values\Column\Physical\NullPhysical;

use function count;
use function Flow\Types\DSL\type_null;

final class AcceptingColumnBuilder implements ColumnBuilder
{
    private int $count = 0;

    public function append(mixed $value): void
    {
        $this->count++;
    }

    public function appendFrom(Column $column, int $i): void
    {
        $this->count++;
    }

    public function appendMany(array $values): void
    {
        $this->count += count($values);
    }

    public function appendPhysicals(array $physicals, ?int $nullCount = null): void
    {
        $this->count += count($physicals);
    }

    public function appendTake(Column $column, array $indices): void
    {
        $this->count += count($indices);
    }

    public function count(): int
    {
        return $this->count;
    }

    public function finish(): Column
    {
        return new ConstantColumn(type_null(), new NullPhysical(), null, $this->count);
    }
}
