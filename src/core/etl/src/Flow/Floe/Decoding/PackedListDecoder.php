<?php

declare(strict_types=1);

namespace Flow\Floe\Decoding;

use function array_values;
use function substr;
use function unpack;

final class PackedListDecoder implements ValueDecoder
{
    /**
     * @param 'e'|'P' $format
     */
    public function __construct(
        private readonly string $format,
    ) {}

    /**
     * @return array<int, float|int>
     */
    public function decode(string $data, int &$position): array
    {
        $count = unpack('V', $data, $position)[1];
        $position += 4;

        if ($count === 0) {
            return [];
        }

        $values = array_values(unpack($this->format . '*', substr($data, $position, $count * 8)));
        $position += $count * 8;

        return $values;
    }
}
