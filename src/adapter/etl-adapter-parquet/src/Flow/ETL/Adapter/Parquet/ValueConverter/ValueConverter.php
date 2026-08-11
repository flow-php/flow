<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\ValueConverter;

interface ValueConverter
{
    public function decode(mixed $value): mixed;

    public function encode(mixed $value): mixed;
}
