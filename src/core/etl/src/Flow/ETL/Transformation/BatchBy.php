<?php

declare(strict_types=1);

namespace Flow\ETL\Transformation;

use Flow\ETL\DataFrame;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Reference;
use Flow\ETL\Transformation;

final readonly class BatchBy implements Transformation
{
    /**
     * @param null|int<1, max> $minSize
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private string|Reference $column,
        private ?int $minSize = null,
    ) {
        if ($this->minSize !== null && $this->minSize <= 0) {
            throw new InvalidArgumentException('Minimum batch size must be greater than 0, given: ' . $this->minSize);
        }
    }

    public function transform(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->batchBy($this->column, $this->minSize);
    }
}
