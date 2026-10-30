<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\IO\Loader;

use Flow\ETL\Values\Rows;

use function interface_exists;

if (interface_exists(JsonEncoder::class, false)) {
    return;
}

interface JsonEncoder
{
    public function encode(Rows $rows, string $separator): string;
}
