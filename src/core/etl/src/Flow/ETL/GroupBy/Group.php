<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy;

final readonly class Group
{
    public function __construct(
        public GroupKey $key,
        public Aggregators $aggregators,
    ) {}
}
