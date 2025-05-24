<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

interface BatchingLoader
{
    /**
     * Returns the default batch size that will be applied when the batch size is not explicitly set through DataFrame::batchSize().
     *
     * @return int<1, max>
     */
    public function defaultBatchSize() : int;
}
