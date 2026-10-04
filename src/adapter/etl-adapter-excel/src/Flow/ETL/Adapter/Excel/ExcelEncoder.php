<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel;

use BackedEnum;
use DateInterval;
use DateTimeInterface;
use Flow\ETL\Column\Column;
use Flow\ETL\Column\Physical\PhysicalFor;
use Flow\ETL\Column\TextValues;
use Flow\ETL\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\TimeType;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\EnumType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\StringType;
use UnitEnum;

use function array_values;
use function count;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_instance_of;

final readonly class ExcelEncoder
{
    public function __construct(
        private string $timeFormat = '%H:%I:%S',
        private TextValues $text = new TextValues(),
    ) {}

    /**
     * A column's cells read once from its physicals: scalars as they are, a datetime or date as a DateTimeInterface
     * (the reader types a cell from its style, a formatted string is any other text), a time in the time format,
     * a backed enum as its backing value, every other type as TextValues renders it.
     *
     * @param Type<mixed> $type
     *
     * @return list<null|bool|DateTimeInterface|float|int|string>
     */
    public function column(Type $type, Column $column): array
    {
        $bare = type_bare($type);
        $physicals = $column->physicals();

        if (
            $bare instanceof StringType
            || $bare instanceof IntegerType
            || $bare instanceof FloatType
            || $bare instanceof BooleanType
        ) {
            /** @var list<null|bool|float|int|string> */
            return $physicals;
        }

        if ($bare instanceof DateTimeType || $bare instanceof DateType || $bare instanceof TimeType) {
            $values = (new PhysicalFor())->type($bare);
            $cells = [];

            // @mago-ignore analysis:mixed-assignment
            foreach ($physicals as $physical) {
                if ($physical === null) {
                    $cells[] = null;

                    continue;
                }

                $cells[] = $bare instanceof TimeType
                    ? type_instance_of(DateInterval::class)
                        ->assert($values->fromPhysical($physical))
                        ->format($this->timeFormat)
                    : type_instance_of(DateTimeInterface::class)->assert($values->fromPhysical($physical));
            }

            return $cells;
        }

        if ($bare instanceof EnumType) {
            $values = (new PhysicalFor())->type($bare);
            $cells = [];

            // @mago-ignore analysis:mixed-assignment
            foreach ($physicals as $physical) {
                $case = $physical === null
                    ? null
                    : type_instance_of(UnitEnum::class)->assert($values->fromPhysical($physical));
                $cells[] = match (true) {
                    $case === null => null,
                    $case instanceof BackedEnum => (string) $case->value,
                    default => $case->name,
                };
            }

            return $cells;
        }

        return $this->text->texts($type, $physicals);
    }

    /**
     * @return list<array<int, null|bool|DateTimeInterface|float|int|string>>
     */
    public function encode(Rows $rows): array
    {
        $columns = [];

        foreach ($rows->schema()->definitions() as $definition) {
            $columns[] = $this->column($definition->type(), $rows->column($definition->entry()->name()));
        }

        $encoded = [];

        for ($i = 0, $count = $rows->count(); $i < $count; $i++) {
            $cells = [];

            for ($c = 0, $width = count($columns); $c < $width; $c++) {
                $cells[] = $columns[$c][$i];
            }

            $encoded[] = $cells;
        }

        return $encoded;
    }

    /**
     * @param list<string> $headers
     *
     * @return list<string>
     */
    public function encodeHeader(array $headers): array
    {
        return array_values($headers);
    }
}
