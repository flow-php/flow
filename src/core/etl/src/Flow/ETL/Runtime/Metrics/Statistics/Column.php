<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Metrics\Statistics;

use DateTimeInterface;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Functions\Reference\UnresolvedReference;
use Flow\ETL\Kernels\Bucketing\KeyEncoder;
use Flow\ETL\Kernels\Bucketing\SeenKeys;
use Flow\ETL\Values\Column\Column as ColumnData;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\StringType;

use function array_diff;
use function array_values;
use function count;
use function Flow\Types\DSL\type_instance_of;
use function is_bool;
use function is_countable;
use function is_float;
use function is_infinite;
use function is_int;
use function is_nan;
use function is_string;
use function max;
use function mb_strlen;
use function min;

final class Column
{
    private readonly SeenKeys $seen;

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
     */
    public function __construct(Definition $definition, ColumnData $column)
    {
        $this->reference = UnresolvedReference::init($definition->name());
        $this->seen = new SeenKeys();
        $this->add($definition, $column);
    }

    /**
     * @param Definition<mixed> $definition
     */
    public function add(Definition $definition, ColumnData $column): void
    {
        if ($this->reference->name() !== $definition->name()) {
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
            $type instanceof DateTimeType, $type instanceof DateType => $this->instants($column),
            $type instanceof ListType, $type instanceof MapType => $this->elements($column->values()),
            default => null,
        };

        // every non-null value of the batch, typed: -0.0 is 0.0, NaN is NaN, datetimes keep their microseconds
        $keys = (new KeyEncoder([$definition->name()]))->of(Rows::fromColumns(
            new Schema($definition),
            [$definition->name() => $column],
            $column->count(),
        ));
        $this->seen->add($column->nullCount() === 0 ? $keys : array_values(array_diff($keys, ["\0n"])));
    }

    public function distinctCount(): int
    {
        return $this->seen->count();
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

    /**
     * A date bound is RFC 3339 text; an infinite float bound, which JSON cannot hold, is "INF" or "-INF".
     *
     * @return array<string, null|bool|float|int|string>
     */
    public function normalize(): array
    {
        $bound = static fn(int|float|DateTimeInterface|bool|null $value): int|float|string|bool|null => match (true) {
            $value instanceof DateTimeInterface => $value->format(DateTimeInterface::RFC3339_EXTENDED),
            is_float($value) && is_infinite($value) => $value > 0 ? 'INF' : '-INF',
            default => $value,
        };

        return [
            'name' => $this->name(),
            'nulls_count' => $this->nullsCount,
            'distinct_count' => $this->distinctCount(),
            'min' => $bound($this->min),
            'max' => $bound($this->max),
            'min_length' => $this->minLength,
            'max_length' => $this->maxLength,
            'min_elements_count' => $this->minElementsCount,
            'max_elements_count' => $this->maxElementsCount,
        ];
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
     * Date and datetime physicals order as their values do: only the two extremes are materialised.
     */
    private function instants(ColumnData $column): void
    {
        $min = null;
        $max = null;

        // @mago-ignore analysis:mixed-assignment
        foreach ($column->physicals() as $i => $physical) {
            if (!is_int($physical)) {
                continue;
            }

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
            // NaN is unordered: as in Parquet statistics and arrow-rs, it is never a bound
            if (is_int($value) || is_float($value) && !is_nan($value) || is_bool($value)) {
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

            $valueLength = mb_strlen($value);
            $this->maxLength = max($this->maxLength ?? $valueLength, $valueLength);
            $this->minLength = min($this->minLength ?? $valueLength, $valueLength);
        }
    }

    /**
     * @param list<mixed> $values
     */
    private function elements(array $values): void
    {
        // @mago-ignore analysis:mixed-assignment
        foreach ($values as $value) {
            if (!is_countable($value)) {
                continue;
            }

            $elementsCount = count($value);
            $this->maxElementsCount = max($this->maxElementsCount ?? $elementsCount, $elementsCount);
            $this->minElementsCount = min($this->minElementsCount ?? $elementsCount, $elementsCount);
        }
    }
}
