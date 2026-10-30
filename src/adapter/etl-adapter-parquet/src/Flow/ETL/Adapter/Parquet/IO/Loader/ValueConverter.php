<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\IO\Loader;

interface ValueConverter
{
    public function encode(mixed $value): mixed;
}
