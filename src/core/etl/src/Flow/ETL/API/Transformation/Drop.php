<?php

declare(strict_types=1);

namespace Flow\ETL\API\Transformation;

use Flow\ETL\API\DataFrame;
use Flow\ETL\API\Transformation;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Functions\Reference\References;
use NoDiscard;

final readonly class Drop implements Transformation
{
    private References $references;

    public function __construct(string|Reference ...$columns)
    {
        $this->references = References::init(...$columns);
    }

    #[NoDiscard]
    public function __invoke(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->drop(...$this->references->all());
    }
}
