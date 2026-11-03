<?php

declare(strict_types=1);

namespace Flow\ETL\API\Pipe;

use Flow\ETL\API\DataFrame;
use Flow\ETL\Flow;
use Flow\ETL\IO\Extractor;
use NoDiscard;

final readonly class Read
{
    public function __construct(
        private Extractor $extractor,
    ) {}

    #[NoDiscard]
    public function __invoke(Flow $flow): DataFrame
    {
        return $flow->read($this->extractor);
    }
}
