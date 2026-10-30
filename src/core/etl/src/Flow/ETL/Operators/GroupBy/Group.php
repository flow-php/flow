<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\GroupBy;

final readonly class Group
{
    public function __construct(
        public GroupKey $key,
        public Aggregators $aggregators,
    ) {}
}
