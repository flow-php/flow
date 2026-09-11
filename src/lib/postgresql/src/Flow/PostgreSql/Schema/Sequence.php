<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

use Flow\PostgreSql\QueryBuilder\Sql;

use function Flow\PostgreSql\DSL\create;

/**
 * @type SequenceShape = array{name: string, data_type: string, start_value: int|string, min_value: int|string, max_value: null|int|string, increment_by: int|string, cycle: bool, cache_value: int|string, owned_by_table: ?string, owned_by_column: ?string}
 */
final readonly class Sequence
{
    public function __construct(
        public string $name,
        public string $dataType = 'bigint',
        public int|string $startValue = 1,
        public int|string $minValue = 1,
        public int|string|null $maxValue = null,
        public int|string $incrementBy = 1,
        public bool $cycle = false,
        public int|string $cacheValue = 1,
        public ?string $ownedByTable = null,
        public ?string $ownedByColumn = null,
    ) {}

    /**
     * @param SequenceShape $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            name: $data['name'],
            dataType: $data['data_type'],
            startValue: $data['start_value'],
            minValue: $data['min_value'],
            maxValue: $data['max_value'] ?? null,
            incrementBy: $data['increment_by'],
            cycle: $data['cycle'],
            cacheValue: $data['cache_value'],
            ownedByTable: $data['owned_by_table'] ?? null,
            ownedByColumn: $data['owned_by_column'] ?? null,
        );
    }

    /**
     * @return SequenceShape
     */
    public function normalize(): array
    {
        return [
            'name' => $this->name,
            'data_type' => $this->dataType,
            'start_value' => $this->startValue,
            'min_value' => $this->minValue,
            'max_value' => $this->maxValue,
            'increment_by' => $this->incrementBy,
            'cycle' => $this->cycle,
            'cache_value' => $this->cacheValue,
            'owned_by_table' => $this->ownedByTable,
            'owned_by_column' => $this->ownedByColumn,
        ];
    }

    public function toSql(): Sql
    {
        $builder = create()
            ->sequence($this->name)
            ->asType($this->dataType)
            ->startWith((int) $this->startValue)
            ->incrementBy((int) $this->incrementBy)
            ->minValue((int) $this->minValue)
            ->cache((int) $this->cacheValue);

        if ($this->maxValue !== null) {
            $builder = $builder->maxValue((int) $this->maxValue);
        } else {
            $builder = $builder->noMaxValue();
        }

        if ($this->cycle) {
            $builder = $builder->cycle();
        }

        if ($this->ownedByTable !== null && $this->ownedByColumn !== null) {
            $builder = $builder->ownedBy($this->ownedByTable, $this->ownedByColumn);
        }

        return $builder;
    }
}
