<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor;

use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Generator;

interface Reader
{
    /**
     * The read of a source with every state resolved (schema, options, inference): the whole plan scan in-process,
     * one split of it in a worker.
     *
     * @return Generator<int, Rows, null|Signal, void>
     */
    public function extract(Backend $backend, Criteria $criteria): Generator;
}
