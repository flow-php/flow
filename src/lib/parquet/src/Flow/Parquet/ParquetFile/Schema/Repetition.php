<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema;

use Flow\Dremel\Repetition as DremelRepetition;

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

    public function toDremel() : DremelRepetition
    {
        return match ($this) {
            self::REQUIRED => DremelRepetition::REQUIRED,
            self::OPTIONAL => DremelRepetition::OPTIONAL,
            self::REPEATED => DremelRepetition::REPEATED,
        };
    }
}
