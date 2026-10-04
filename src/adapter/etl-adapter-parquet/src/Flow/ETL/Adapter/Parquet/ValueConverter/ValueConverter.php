<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\ValueConverter;

interface ValueConverter
{
    public function encode(mixed $value): mixed;
}
