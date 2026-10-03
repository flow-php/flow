<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use Flow\ETL\Rows;

use function extension_loaded;

if (extension_loaded('flow_php')) {
    return;
}

interface JsonEncoder
{
    public function encode(Rows $rows, string $separator): string;
}
