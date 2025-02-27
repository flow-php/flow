<?php

declare(strict_types=1);

namespace Flow\ETL\Transformation;

use Flow\ETL\{DataFrame, Transformation};

final readonly class Limit implements Transformation
{
    public function __construct(private ?int $limit)
    {
    }

    public function transform(DataFrame $dataFrame) : DataFrame
    {
        return $dataFrame->limit($this->limit);
    }
}
