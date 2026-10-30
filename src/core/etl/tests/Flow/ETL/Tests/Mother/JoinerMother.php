<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Functions\Join\Expression;
use Flow\ETL\Kernels\HashJoin\Joiner;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Column\AdaptiveBackend;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Join\JoinType;

final class JoinerMother
{
    /**
     * The joiner the join operators build for $expression: its schema, keys and condition.
     */
    public static function of(
        Expression $expression,
        JoinType $type,
        Backend $backend = new AdaptiveBackend(),
        BatchSize $batchSize = new BatchSize(1000),
    ): Joiner {
        return new Joiner($type, $expression->schema($type), $expression->keys(), $expression, $backend, $batchSize);
    }
}
