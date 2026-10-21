<?php

declare(strict_types=1);

namespace Flow\Parquet\Data;

use function strspn;
use function substr;

final readonly class DeltaByteArrayEncoder
{
    public function __construct(
        private DeltaBinaryPackedEncoder $prefixLengths = new DeltaBinaryPackedEncoder(),
        private DeltaLengthByteArrayEncoder $suffixes = new DeltaLengthByteArrayEncoder(),
    ) {}

    /**
     * @param list<string> $values
     */
    public function encode(array $values): string
    {
        $prefixLengths = [];
        $suffixes = [];
        $previous = '';

        foreach ($values as $value) {
            // string XOR stops at the shorter operand: leading "\0" bytes are the common prefix
            $prefixLength = strspn($previous ^ $value, "\0");
            $prefixLengths[] = $prefixLength;
            $suffixes[] = substr($value, $prefixLength);
            $previous = $value;
        }

        return $this->prefixLengths->encode($prefixLengths) . $this->suffixes->encode($suffixes);
    }
}
