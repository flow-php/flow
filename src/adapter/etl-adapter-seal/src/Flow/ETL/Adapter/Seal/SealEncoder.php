<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal;

use DateTimeInterface;
use Flow\ETL\Column\Column;
use Flow\ETL\Column\Physical\IdentityPhysical;
use Flow\ETL\Column\Physical\PhysicalFor;
use Flow\ETL\Column\TextValues;
use Flow\ETL\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Logical\TimeType;
use Flow\Types\Type\Native\ArrayType;
use Flow\Types\Type\Native\NullType;
use Flow\Types\Value\Json;

use function array_keys;
use function Flow\Types\DSL\type_bare;
use function is_array;
use function is_bool;
use function is_float;
use function is_int;
use function is_string;

final class SealEncoder
{
    private readonly TextValues $text;

    public function __construct(
        private readonly string $dateTimeFormat = DateTimeInterface::ATOM,
        private readonly string $dateFormat = 'Y-m-d',
    ) {
        $this->text = new TextValues($dateTimeFormat, $dateFormat);
    }

    /**
     * @return list<array<string, mixed>>
     */
    public function encode(Rows $rows): array
    {
        $columns = [];

        foreach ($rows->schema()->definitions() as $definition) {
            $name = $definition->entry()->name();
            $columns[$name] = $this->column($definition->type(), $rows->column($name));
        }

        $documents = [];

        for ($i = 0, $count = $rows->count(); $i < $count; $i++) {
            $document = [];

            foreach ($columns as $name => $column) {
                $document[$name] = $column[$i];
            }

            $documents[] = $document;
        }

        return $documents;
    }

    /**
     * A column's document fields: datetime / date text from the physicals, json and containers as normalized arrays,
     * physicals where the physical is the value (scalars, a time's microseconds, null), TextValues for every other type.
     *
     * @param Type<mixed> $type
     *
     * @return list<mixed>
     */
    public function column(Type $type, Column $column): array
    {
        $bare = type_bare($type);

        if ($bare instanceof DateTimeType || $bare instanceof DateType) {
            /** @var list<?int> $physicals */
            $physicals = $column->physicals();

            return $this->text->dateTimes(
                $bare,
                $physicals,
                $bare instanceof DateType ? $this->dateFormat : $this->dateTimeFormat,
            );
        }

        if ($bare instanceof JsonType) {
            $fields = [];

            // @mago-ignore analysis:mixed-assignment
            foreach ($column->values() as $value) {
                $fields[] = $value instanceof Json ? $this->normalizeArray($value->toArray()) : null;
            }

            return $fields;
        }

        if (
            $bare instanceof ListType
            || $bare instanceof MapType
            || $bare instanceof StructureType
            || $bare instanceof ArrayType
        ) {
            $fields = [];

            // @mago-ignore analysis:mixed-assignment
            foreach ($column->values() as $value) {
                $fields[] = $this->normalizeArray($value);
            }

            return $fields;
        }

        if (
            $bare instanceof TimeType
            || $bare instanceof NullType
            || (new PhysicalFor())->type($bare) instanceof IdentityPhysical
        ) {
            return $column->physicals();
        }

        return $this->text->texts($type, $column->physicals());
    }

    /**
     * @return null|array<array-key, mixed>
     */
    private function normalizeArray(mixed $value): ?array
    {
        if (!is_array($value)) {
            return null;
        }

        $normalized = [];

        foreach (array_keys($value) as $key) {
            $normalized[$key] = $this->normalizeValue($value[$key]);
        }

        return $normalized;
    }

    private function normalizeValue(mixed $value): string|float|int|bool|array|null
    {
        if ($value instanceof DateTimeInterface) {
            return $value->format($this->dateTimeFormat);
        }

        if (is_array($value)) {
            return $this->normalizeArray($value);
        }

        if (is_string($value) || is_int($value) || is_float($value) || is_bool($value)) {
            return $value;
        }

        return null;
    }
}
