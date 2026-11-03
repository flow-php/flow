<?php

declare(strict_types=1);

namespace Flow\ETL\API\Transformation;

use Flow\ETL\API\Branch;
use Flow\ETL\API\DataFrame;
use Flow\ETL\API\Transactional;
use Flow\ETL\API\Transformation;
use Flow\ETL\IO\Loader;
use NoDiscard;

final readonly class Write implements Transformation
{
    public function __construct(
        private Loader|Branch|Transactional $write,
    ) {}

    #[NoDiscard]
    public function __invoke(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->write($this->write);
    }
}
