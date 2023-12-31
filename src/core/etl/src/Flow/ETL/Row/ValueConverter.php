<?php declare(strict_types=1);

namespace Flow\ETL\Row;

interface ValueConverter
{
    public function convert(mixed $value) : mixed;
}
