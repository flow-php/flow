<?php

declare(strict_types=1);

namespace Flow\ETL\Column;

use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\HTMLElementType;
use Flow\Types\Type\Logical\HTMLType;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\ListType;
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
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_mixed;

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
        return $this->equalByPhysical($column->type())
            ? $this->equalities($column->type(), $column->physicals())
            : $column->values();
    }

    /**
     * Element physicals carry their owner document, so elements, and lists of them, compare by their own markup.
     *
     * @param Type<mixed> $type
     * @param list<mixed> $physicals
     *
     * @return list<mixed> === on these is value equality
     */
    public function equalities(Type $type, array $physicals): array
    {
        $bare = type_bare($type);
        $element = $bare instanceof ListType ? type_bare($bare->element()) : $bare;

        if ($bare instanceof DateType) {
            return $this->instants($physicals);
        }

        if ($element instanceof DateType) {
            $lists = [];

            // @mago-ignore analysis:mixed-assignment
            foreach ($physicals as $list) {
                $lists[] = $list === null ? null : $this->instants(type_list(type_mixed())->assert($list));
            }

            return $lists;
        }

        return $element instanceof HTMLElementType || $element instanceof XMLElementType
            ? (new TextValues())->of($type, $physicals)
            : $physicals;
    }

    /**
     * A date's physical days on a datetime's microsecond scale, so a date and a datetime at the same instant compare
     * and hash equal.
     *
     * @param list<mixed> $days
     *
     * @return list<?int>
     */
    public function instants(array $days): array
    {
        $instants = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($days as $day) {
            $instants[] = $day === null ? null : type_integer()->assert($day) * 86_400_000_000;
        }

        return $instants;
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
        if (!$this->orderedByPhysical($column->type())) {
            return $column->values();
        }

        return type_bare($column->type()) instanceof DateType
            ? $this->instants($column->physicals())
            : $column->physicals();
    }
}
