<?php

declare(strict_types=1);

namespace Flow\ETL\API\Transformation;

use Flow\ETL\API\DataFrame;
use Flow\ETL\API\Transformation;
use Flow\ETL\Functions\Literal;

/**
 * A missing column is added holding the mask.
 */
final readonly class MaskColumns implements Transformation
{
    /**
     * @param array<int, string> $columns
     */
    public function __construct(
        private array $columns = [],
        private string $mask = '******',
    ) {}

    public function transform(DataFrame $dataFrame): DataFrame
    {
        foreach ($this->columns as $column) {
            $dataFrame = $dataFrame->withColumn($column, new Literal($this->mask));
        }

        return $dataFrame;
    }
}
