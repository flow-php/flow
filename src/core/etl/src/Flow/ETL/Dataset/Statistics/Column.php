<?php

declare(strict_types=1);

namespace Flow\ETL\Dataset\Statistics;

use function Flow\Types\DSL\type_instance_of;
use Flow\ETL\Row\Entry\{BooleanEntry, DateEntry, DateTimeEntry, FloatEntry, IntegerEntry, ListEntry, MapEntry, StringEntry, StructureEntry, UuidEntry};
use Flow\ETL\Row\{Entry, Reference};
use Flow\Types\Value\Uuid;

final class Column
{
    private readonly DistinctCounter $distinctCounter;

    private int|float|\DateTimeInterface|bool|null $max = null;

    private ?int $maxElementsCount = null;

    private ?int $maxLength = null;

    private int|float|\DateTimeInterface|bool|null $min = null;

    private ?int $minElementsCount = null;

    private ?int $minLength = null;

    private int $nullsCount = 0;

    private readonly Reference $reference;

    /**
     * @param Entry<mixed> $entry
     *
     * @throws \JsonException
     */
    public function __construct(Entry $entry)
    {
        $this->reference = $entry->ref();
        $this->distinctCounter = new DistinctCounter();
        $this->calculate($entry);
    }

    /**
     * @param Entry<mixed> $entry
     */
    public function calculate(Entry $entry) : void
    {
        if (!$this->reference->is($entry->ref())) {
            return;
        }

        $value = $entry->value();

        if ($value === null) {
            $this->nullsCount++;

            return;
        }

        if ($entry instanceof UuidEntry) {
            $this->distinctCounter->add(type_instance_of(Uuid::class)->assert($value)->toString());

            return;
        }

        if ($entry instanceof StructureEntry) {
            $this->distinctCounter->add(\json_encode($value, JSON_THROW_ON_ERROR));

            return;
        }

        if ($entry instanceof ListEntry || $entry instanceof MapEntry) {
            $this->distinctCounter->add(\json_encode($value, JSON_THROW_ON_ERROR));
            $elementsCount = \is_countable($value) ? \count($value) : 0;
            $this->maxElementsCount = \max($this->maxElementsCount ?? $elementsCount, $elementsCount);
            $this->minElementsCount = \min($this->minElementsCount ?? $elementsCount, $elementsCount);

            return;
        }

        if (\is_scalar($value) || $value instanceof \DateTimeInterface) {
            $this->distinctCounter->add($value);
        }

        if ($entry instanceof StringEntry) {
            $valueLength = \mb_strlen(\is_scalar($entry->value()) ? (string) $entry->value() : '');
            $this->maxLength = \max($this->maxLength ?? $valueLength, $valueLength);
            $this->minLength = \min($this->minLength ?? $valueLength, $valueLength);

            return;
        }

        if ($entry instanceof DateEntry || $entry instanceof DateTimeEntry) {
            if ($value instanceof \DateTimeInterface) {
                $this->max = \max($this->max ?? $value, $value);
                $this->min = \min($this->min ?? $value, $value);
            }

            return;
        }

        if ($entry instanceof IntegerEntry || $entry instanceof FloatEntry || $entry instanceof BooleanEntry) {
            if (\is_int($value) || \is_float($value) || \is_bool($value)) {
                $this->min = \min($this->min ?? $value, $value);
                $this->max = \max($this->max ?? $value, $value);
            }
        }
    }

    public function distinctCount() : int
    {
        return $this->distinctCounter->count();
    }

    public function max() : int|float|\DateTimeInterface|bool|null
    {
        return $this->max;
    }

    public function maxElementsCount() : ?int
    {
        return $this->maxElementsCount;
    }

    public function maxLength() : ?int
    {
        return $this->maxLength;
    }

    public function min() : int|float|\DateTimeInterface|bool|null
    {
        return $this->min;
    }

    public function minElementsCount() : ?int
    {
        return $this->minElementsCount;
    }

    public function minLength() : ?int
    {
        return $this->minLength;
    }

    public function name() : string
    {
        return $this->reference->name();
    }

    public function nullCount() : int
    {
        return $this->nullsCount;
    }

    public function reference() : Reference
    {
        return $this->reference;
    }
}
