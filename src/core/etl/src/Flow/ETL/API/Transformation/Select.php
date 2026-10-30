<?php

declare(strict_types=1);

namespace Flow\ETL\API\Transformation;

use Flow\ETL\API\DataFrame;
use Flow\ETL\API\Transformation;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Functions\Reference\References;

final readonly class Select implements Transformation
{
    private References $references;

    public function __construct(string|Reference ...$columns)
    {
        $this->references = References::init(...$columns);
    }

    public function transform(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->select(...$this->references->all());
    }
}
