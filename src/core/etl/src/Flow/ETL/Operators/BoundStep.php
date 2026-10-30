<?php

declare(strict_types=1);

namespace Flow\ETL\Operators;

use Flow\ETL\Operators\Join\CrossJoin;
use Flow\ETL\Operators\Join\HashJoin;
use Flow\ETL\Values\Schema;

/**
 * @template-covariant T of Processor|Transformer|HashJoin|CrossJoin
 */
final readonly class BoundStep
{
    /**
     * @param T $step
     */
    public function __construct(
        public Processor|Transformer|HashJoin|CrossJoin $step,
        public Schema $output,
    ) {}
}
