<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

use function Flow\PostgreSql\DSL\create;

use Flow\PostgreSql\QueryBuilder\SqlQuery;

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
    ) {
    }

    public function toSql() : SqlQuery
    {
        $builder = create()->sequence($this->name)
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
