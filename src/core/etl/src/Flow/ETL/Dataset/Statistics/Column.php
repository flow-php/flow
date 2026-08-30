<?php

declare(strict_types=1);

namespace Flow\ETL\Dataset\Statistics;

use DateTimeInterface;
use DateTimeZone;
use Flow\ETL\Row\Reference;
use Flow\ETL\Schema\Definition;
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
use function is_bool;
use function is_countable;
use function is_float;
use function is_int;
use function is_scalar;
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
    public function __construct(Definition $definition, mixed $value)
    {
        $this->reference = $definition->entry();
        $this->distinctCounter = new DistinctCounter();
        $this->add($definition, $value);
    }

    /**
     * @param Definition<mixed> $definition
     */
    public function add(Definition $definition, mixed $value): void
    {
        if ($this->reference->name() !== $definition->entry()->name()) {
            return;
        }

        $type = $definition->type();

        if ($value === null) {
            $this->nullsCount++;

            return;
        }

        if ($type instanceof UuidType) {
            $this->distinctCounter->add(type_instance_of(Uuid::class)->assert($value)->toString());

            return;
        }

        if ($type instanceof TimeZoneType) {
            $this->distinctCounter->add(type_instance_of(DateTimeZone::class)->assert($value)->getName());

            return;
        }

        if ($type instanceof StructureType) {
            $this->distinctCounter->add(json_encode($value, JSON_THROW_ON_ERROR));

            return;
        }

        if ($type instanceof ListType || $type instanceof MapType) {
            $this->distinctCounter->add(json_encode($value, JSON_THROW_ON_ERROR));
            $elementsCount = is_countable($value) ? count($value) : 0;
            $this->maxElementsCount = max($this->maxElementsCount ?? $elementsCount, $elementsCount);
            $this->minElementsCount = min($this->minElementsCount ?? $elementsCount, $elementsCount);

            return;
        }

        if (is_scalar($value) || $value instanceof DateTimeInterface) {
            $this->distinctCounter->add($value);
        }

        if ($type instanceof StringType) {
            $valueLength = mb_strlen(is_scalar($value) ? (string) $value : '');
            $this->maxLength = max($this->maxLength ?? $valueLength, $valueLength);
            $this->minLength = min($this->minLength ?? $valueLength, $valueLength);

            return;
        }

        if ($type instanceof DateType || $type instanceof DateTimeType) {
            if ($value instanceof DateTimeInterface) {
                $this->max = max($this->max ?? $value, $value);
                $this->min = min($this->min ?? $value, $value);
            }

            return;
        }

        if ($type instanceof IntegerType || $type instanceof FloatType || $type instanceof BooleanType) {
            if (is_int($value) || is_float($value) || is_bool($value)) {
                $this->min = min($this->min ?? $value, $value);
                $this->max = max($this->max ?? $value, $value);
            }
        }
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
}
