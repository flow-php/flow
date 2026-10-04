<?php

declare(strict_types=1);

namespace Flow\ETL\Column;

use Flow\ETL\Column\Layout\Buffers;
use Flow\ETL\Column\Php\CastingColumnBuilder;
use Flow\ETL\Column\Php\ColumnDecoder;
use Flow\ETL\Column\Php\ConstantColumn;
use Flow\ETL\Column\Php\ListColumn;
use Flow\ETL\Column\Php\MapColumn;
use Flow\ETL\Column\Php\ScalarColumn;
use Flow\ETL\Column\Php\StructColumn;
use Flow\ETL\Column\Physical\PhysicalBuilderFor;
use Flow\ETL\Column\Physical\PhysicalFor;
use Flow\ETL\Exception\ColumnMismatchException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Definition\NullDefinition;

use function range;
use function sprintf;

final readonly class PhpBackend implements Backend
{
    public function builder(Definition $definition): ColumnBuilder
    {
        return new CastingColumnBuilder(
            $definition,
            (new PhysicalFor())->definition($definition),
            (new PhysicalBuilderFor())->type($definition->type()),
        );
    }

    public function constant(Definition $definition, mixed $value, int $count): Column
    {
        $physical = (new PhysicalFor())->definition($definition);

        if ($value === null) {
            if (!$definition->isNullable() && !$definition instanceof NullDefinition) {
                throw ColumnMismatchException::valueDoesNotMatch($definition, null);
            }

            return new ConstantColumn($definition->type(), $physical, null, $count);
        }

        return new ConstantColumn(
            $definition->type(),
            $physical,
            $physical->toPhysical($definition->type()->cast($value)),
            $count,
        );
    }

    public function decode(Definition $definition, array $buffers, int $count, int $nullCount): Column
    {
        $cursor = new Buffers($buffers);
        $column = (new ColumnDecoder())->decode($definition->type(), $cursor, $count, $nullCount);

        if ($cursor->remaining() !== 0) {
            throw new InvalidArgumentException(sprintf(
                'Column "%s": %d buffers left after decoding',
                $definition->entry()->name(),
                $cursor->remaining(),
            ));
        }

        return $column;
    }

    public function adopt(Definition $definition, Column $column): Column
    {
        if ($column instanceof ValueColumn) {
            throw ColumnMismatchException::untypedColumn($definition);
        }

        if (
            $column instanceof ScalarColumn
            || $column instanceof ConstantColumn
            || $column instanceof ListColumn
            || $column instanceof MapColumn
            || $column instanceof StructColumn
        ) {
            return $column;
        }

        $builder = $this->builder($definition);

        if ($column->count() > 0) {
            $builder->appendTake($column, range(0, $column->count() - 1));
        }

        return $builder->finish();
    }

    public function allocatedBytes(): int
    {
        return 0;
    }
}
