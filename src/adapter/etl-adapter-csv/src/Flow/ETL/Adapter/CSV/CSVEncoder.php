<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Rows;

interface CSVEncoder
{
    public function encode(Rows $rows): string;

    /**
     * @param list<string> $headers
     */
    public function encodeHeader(array $headers): string;
}
