<?php

declare(strict_types=1);

namespace Flow\ETL\Column;

use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\HTMLElementType;
use Flow\Types\Type\Logical\HTMLType;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\TimeType;
use Flow\Types\Type\Logical\UuidType;
use Flow\Types\Type\Logical\XMLElementType;
use Flow\Types\Type\Logical\XMLType;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\EnumType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\StringType;

use function Flow\Types\DSL\type_bare;

final readonly class ComparableValues
{
    /**
     * @param Type<mixed> $type
     */
    public function equalByPhysical(Type $type): bool
    {
        $bare = type_bare($type);

        return (
            $this->orderedByPhysical($bare)
            || $bare instanceof UuidType
            || $bare instanceof JsonType
            || $bare instanceof EnumType
            || $bare instanceof XMLType
            || $bare instanceof XMLElementType
            || $bare instanceof HTMLType
            || $bare instanceof HTMLElementType
        );
    }

    /**
     * @return list<mixed> === on these is value equality
     */
    public function equality(Column $column): array
    {
        return $this->equalByPhysical($column->type()) ? $column->physicals() : $column->values();
    }

    /**
     * @param Type<mixed> $type
     */
    public function orderedByPhysical(Type $type): bool
    {
        $bare = type_bare($type);

        return (
            $bare instanceof IntegerType
            || $bare instanceof FloatType
            || $bare instanceof BooleanType
            || $bare instanceof StringType
            || $bare instanceof DateTimeType
            || $bare instanceof DateType
            || $bare instanceof TimeType
        );
    }

    /**
     * @return list<mixed> PHP <, <=, <=> on these equal today's comparison of the values
     */
    public function ordering(Column $column): array
    {
        return $this->orderedByPhysical($column->type()) ? $column->physicals() : $column->values();
    }
}
