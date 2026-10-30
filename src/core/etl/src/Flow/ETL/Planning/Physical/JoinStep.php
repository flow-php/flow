<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Physical;

use Flow\ETL\Operators\Join\CrossJoin;
use Flow\ETL\Operators\Join\HashJoin;

final readonly class JoinStep
{
    public function __construct(
        public HashJoin|CrossJoin $join,
        public PhysicalPlan $right,
    ) {}
}
