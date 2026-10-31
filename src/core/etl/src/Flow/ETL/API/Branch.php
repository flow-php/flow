<?php

declare(strict_types=1);

namespace Flow\ETL\API;

interface Branch
{
    /**
     * @param DataFrame $prefix a frame over the caller's chain - the writes of the returned frame become the caller's
     */
    public function write(DataFrame $prefix): DataFrame;
}
