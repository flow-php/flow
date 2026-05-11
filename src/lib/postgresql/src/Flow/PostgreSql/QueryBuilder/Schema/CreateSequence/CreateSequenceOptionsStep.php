<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\CreateSequence;

interface CreateSequenceOptionsStep extends CreateSequenceFinalStep
{
    public function asType(string $dataType): self;

    public function cache(int $cache): self;

    public function cycle(): self;

    public function incrementBy(int $increment): self;

    public function maxValue(int $maxValue): self;

    public function minValue(int $minValue): self;

    public function noCycle(): self;

    public function noMaxValue(): self;

    public function noMinValue(): self;

    public function ownedBy(string $table, string $column): self;

    public function ownedByNone(): self;

    public function startWith(int $start): self;
}
