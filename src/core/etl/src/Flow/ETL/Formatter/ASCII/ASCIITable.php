<?php

declare(strict_types=1);

namespace Flow\ETL\Formatter\ASCII;

use Flow\ETL\Rows;

final class ASCIITable
{
    public function __construct(private readonly Rows $rows)
    {
    }

    public function print(int|bool $truncate = 20) : string
    {
        $headers = new Headers($this->rows);
        $body = new Body($this->rows);

        return (new ASCIIHeaders($headers, $body))->print($truncate)
            . (new ASCIIBody($headers, $body))->print($truncate);
    }
}
