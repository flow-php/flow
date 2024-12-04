<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema;

enum Repetition : int
{
    case OPTIONAL = 1;
    case REPEATED = 2;
    case REQUIRED = 0;

    public function isOptional() : bool
    {
        return $this === self::OPTIONAL;
    }

    public function isRepeated() : bool
    {
        return $this === self::REPEATED;
    }

    public function isRequired() : bool
    {
        return $this === self::REQUIRED;
    }
}
