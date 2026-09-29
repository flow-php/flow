<?php

declare(strict_types=1);

namespace Flow\ETL\Dataset\Statistics;

use DateTimeInterface;
use DateTimeZone;
use Flow\ETL\Column\Column as ColumnData;
use Flow\ETL\Row\Reference;
use Flow\ETL\Schema\Definition;
use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Logical\TimeZoneType;
use Flow\Types\Type\Logical\UuidType;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\StringType;
use Flow\Types\Value\Uuid;

use function count;
use function Flow\Types\DSL\type_instance_of;
use function intdiv;
use function is_bool;
use function is_countable;
use function is_float;
use function is_int;
use function is_scalar;
use function is_string;
use function json_encode;
use function max;
use function mb_strlen;
use function min;

final class Column
{
    private readonly DistinctCounter $distinctCounter;

    private int|float|DateTimeInterface|bool|null $max = null;

    private ?int $maxElementsCount = null;

    private ?int $maxLength = null;

    private int|float|DateTimeInterface|bool|null $min = null;

    private ?int $minElementsCount = null;

    private ?int $minLength = null;

    private int $nullsCount = 0;

    private readonly Reference $reference;

    /**
     * @param Definition<mixed> $definition
     *
     * @throws \JsonException
     */
    public function __construct(Definition $definition, ColumnData $column)
    {
        $this->reference = $definition->entry();
        $this->distinctCounter = new DistinctCounter();
        $this->add($definition, $column);
    }

    /**
     * @param Definition<mixed> $definition
     *
     * @throws \JsonException
     */
    public function add(Definition $definition, ColumnData $column): void
    {
        if ($this->reference->name() !== $definition->entry()->name()) {
            return;
        }

        $this->nullsCount += $column->nullCount();

        if ($column->nullCount() === $column->count()) {
            return;
        }

        $type = $definition->type();

        match (true) {
            $type instanceof StringType => $this->strings($column->physicals()),
            $type instanceof IntegerType, $type instanceof FloatType, $type instanceof BooleanType => $this->scalars(
                $column->physicals(),
            ),
            $type instanceof DateTimeType => $this->instants($column, 1_000_000),
            $type instanceof DateType => $this->instants($column, 1),
            default => $this->values($type, $column->values()),
        };
    }

    public function distinctCount(): int
    {
        return $this->distinctCounter->count();
    }

    public function max(): int|float|DateTimeInterface|bool|null
    {
        return $this->max;
    }

    public function maxElementsCount(): ?int
    {
        return $this->maxElementsCount;
    }

    public function maxLength(): ?int
    {
        return $this->maxLength;
    }

    public function min(): int|float|DateTimeInterface|bool|null
    {
        return $this->min;
    }

    public function minElementsCount(): ?int
    {
        return $this->minElementsCount;
    }

    public function minLength(): ?int
    {
        return $this->minLength;
    }

    public function name(): string
    {
        return $this->reference->name();
    }

    public function nullCount(): int
    {
        return $this->nullsCount;
    }

    public function reference(): Reference
    {
        return $this->reference;
    }

    /**
     * Date physicals are days, datetime physicals microseconds: the distinct key is the Unix second, as
     * DateTimeInterface::getTimestamp() gives it, and only the two extremes are materialised.
     *
     * @param int $perSecond physical units per second, or 1 for days
     */
    private function instants(ColumnData $column, int $perSecond): void
    {
        $min = null;
        $max = null;

        // @mago-ignore analysis:mixed-assignment
        foreach ($column->physicals() as $i => $physical) {
            if (!is_int($physical)) {
                continue;
            }

            $seconds = $perSecond === 1 ? $physical * 86_400 : intdiv($physical, $perSecond);

            if ($perSecond !== 1 && ($physical % $perSecond) < 0) {
                $seconds--;
            }

            $this->distinctCounter->add($seconds);

            if ($min === null || $physical < $min[1]) {
                $min = [$i, $physical];
            }

            if ($max === null || $physical > $max[1]) {
                $max = [$i, $physical];
            }
        }

        if ($min === null || $max === null) {
            return;
        }

        $minValue = type_instance_of(DateTimeInterface::class)->assert($column->value($min[0]));
        $maxValue = type_instance_of(DateTimeInterface::class)->assert($column->value($max[0]));
        $this->min = min($this->min ?? $minValue, $minValue);
        $this->max = max($this->max ?? $maxValue, $maxValue);
    }

    /**
     * @param list<mixed> $physicals
     */
    private function scalars(array $physicals): void
    {
        // @mago-ignore analysis:mixed-assignment
        foreach ($physicals as $value) {
            if (is_int($value) || is_float($value) || is_bool($value)) {
                $this->distinctCounter->add($value);
                $this->min = min($this->min ?? $value, $value);
                $this->max = max($this->max ?? $value, $value);
            }
        }
    }

    /**
     * @param list<mixed> $physicals
     */
    private function strings(array $physicals): void
    {
        // @mago-ignore analysis:mixed-assignment
        foreach ($physicals as $value) {
            if (!is_string($value)) {
                continue;
            }

            $this->distinctCounter->add($value);
            $valueLength = mb_strlen($value);
            $this->maxLength = max($this->maxLength ?? $valueLength, $valueLength);
            $this->minLength = min($this->minLength ?? $valueLength, $valueLength);
        }
    }

    /**
     * @param Type<mixed> $type
     * @param list<mixed> $values
     *
     * @throws \JsonException
     */
    private function values(Type $type, array $values): void
    {
        // @mago-ignore analysis:mixed-assignment
        foreach ($values as $value) {
            if ($value === null) {
                continue;
            }

            if ($type instanceof UuidType) {
                $this->distinctCounter->add(type_instance_of(Uuid::class)->assert($value)->toString());

                continue;
            }

            if ($type instanceof TimeZoneType) {
                $this->distinctCounter->add(type_instance_of(DateTimeZone::class)->assert($value)->getName());

                continue;
            }

            if ($type instanceof StructureType) {
                $this->distinctCounter->add(json_encode($value, JSON_THROW_ON_ERROR));

                continue;
            }

            if ($type instanceof ListType || $type instanceof MapType) {
                $this->distinctCounter->add(json_encode($value, JSON_THROW_ON_ERROR));
                $elementsCount = is_countable($value) ? count($value) : 0;
                $this->maxElementsCount = max($this->maxElementsCount ?? $elementsCount, $elementsCount);
                $this->minElementsCount = min($this->minElementsCount ?? $elementsCount, $elementsCount);

                continue;
            }

            if (is_scalar($value) || $value instanceof DateTimeInterface) {
                $this->distinctCounter->add($value);
            }
        }
    }
}
