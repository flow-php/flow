<?php

declare(strict_types=1);

namespace Flow\ETL\API;

interface Branch
{
    /**
     * @param DataFrame $prefix a frame over the caller's chain, handed to this call only - its writes become the
     *                         caller's writes, its other verbs never reach the caller
     */
    public function write(DataFrame $prefix): void;
}
