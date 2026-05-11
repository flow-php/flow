<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

use Flow\PostgreSql\QueryBuilder\Sql;
use Flow\PostgreSql\Schema\Sequence;

use function Flow\PostgreSql\DSL\alter;

final readonly class SequenceDiff implements Diff
{
    public function __construct(
        public Sequence $source,
        public Sequence $target,
    ) {}

    /**
     * @return list<Sql>
     */
    public function generate(): array
    {
        $builder = alter()->sequence($this->target->name);
        $hasChanges = false;

        if ($this->target->dataType !== $this->source->dataType) {
            $builder = $builder->asType($this->target->dataType);
            $hasChanges = true;
        }

        if ($this->target->incrementBy !== $this->source->incrementBy) {
            $builder = $builder->incrementBy((int) $this->target->incrementBy);
            $hasChanges = true;
        }

        if ($this->target->startValue !== $this->source->startValue) {
            $builder = $builder->startWith((int) $this->target->startValue);
            $hasChanges = true;
        }

        if ($this->target->minValue !== $this->source->minValue) {
            $builder = $builder->minValue((int) $this->target->minValue);
            $hasChanges = true;
        }

        if ($this->target->maxValue !== $this->source->maxValue) {
            if ($this->target->maxValue === null) {
                $builder = $builder->noMaxValue();
            } else {
                $builder = $builder->maxValue((int) $this->target->maxValue);
            }
            $hasChanges = true;
        }

        if ($this->target->cacheValue !== $this->source->cacheValue) {
            $builder = $builder->cache((int) $this->target->cacheValue);
            $hasChanges = true;
        }

        if ($this->target->cycle !== $this->source->cycle) {
            $builder = $this->target->cycle ? $builder->cycle() : $builder->noCycle();
            $hasChanges = true;
        }

        if (
            $this->target->ownedByTable !== $this->source->ownedByTable
            || $this->target->ownedByColumn !== $this->source->ownedByColumn
        ) {
            if ($this->target->ownedByTable !== null && $this->target->ownedByColumn !== null) {
                $builder = $builder->ownedBy($this->target->ownedByTable, $this->target->ownedByColumn);
            } else {
                $builder = $builder->ownedByNone();
            }
            $hasChanges = true;
        }

        if (!$hasChanges) {
            return [];
        }

        return [$builder];
    }
}
