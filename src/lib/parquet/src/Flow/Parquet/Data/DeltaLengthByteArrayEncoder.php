<?php

declare(strict_types=1);

namespace Flow\Parquet\Data;

use function array_map;
use function implode;
use function strlen;

final readonly class DeltaLengthByteArrayEncoder
{
    public function __construct(
        private DeltaBinaryPackedEncoder $lengths = new DeltaBinaryPackedEncoder(),
    ) {}

    /**
     * @param list<string> $values
     */
    public function encode(array $values): string
    {
        return $this->lengths->encode(array_map(strlen(...), $values)) . implode('', $values);
    }
}
