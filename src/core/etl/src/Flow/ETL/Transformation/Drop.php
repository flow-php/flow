<?php

declare(strict_types=1);

namespace Flow\ETL\Transformation;

use Flow\ETL\DataFrame;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\ETL\Transformation;

final readonly class Drop implements Transformation
{
    private References $references;

    public function __construct(string|Reference ...$entries)
    {
        $this->references = References::init(...$entries);
    }

    public function transform(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->drop(...$this->references->all());
    }
}
