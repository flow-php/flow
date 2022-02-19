<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Cast\ArrayEntryEach;

use Flow\ETL\Transformer\Cast\CastArrayEntryEach;
use Flow\ETL\Transformer\Cast\ValueCaster;

/**
 * @psalm-immutable
 */
final class CastArrayEntryEachToDateTime extends CastArrayEntryEach
{
    public function __construct(string $arrayEntryName, ?string $timeZone = null, ?string $toTimeZone = null)
    {
        parent::__construct($arrayEntryName, new ValueCaster\StringToDateTimeCaster($timeZone, $toTimeZone));
    }

    public static function nullable(string $arrayEntryName, ?string $timeZone = null, ?string $toTimeZone = null) : self
    {
        return new self($arrayEntryName, $timeZone, $toTimeZone);
    }
}
