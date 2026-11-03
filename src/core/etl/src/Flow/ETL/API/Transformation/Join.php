<?php

declare(strict_types=1);

namespace Flow\ETL\API\Transformation;

use Flow\ETL\API\DataFrame;
use Flow\ETL\API\Transformation;
use Flow\ETL\Functions\Join\Expression;
use Flow\ETL\Planning\Logical\Node\JoinAlgorithmBuilder;
use Flow\ETL\Values\Join\JoinType;
use NoDiscard;

final readonly class Join implements Transformation
{
    /**
     * @param null|JoinAlgorithmBuilder $algorithm null defers to configuration; a builder pins the algorithm
     *                                            for this operation and skips any automatic choice
     */
    public function __construct(
        private DataFrame $dataFrame,
        private Expression $on,
        private string|JoinType $type = JoinType::left,
        private ?JoinAlgorithmBuilder $algorithm = null,
    ) {}

    #[NoDiscard]
    public function __invoke(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->join($this->dataFrame, $this->on, $this->type, $this->algorithm);
    }
}
