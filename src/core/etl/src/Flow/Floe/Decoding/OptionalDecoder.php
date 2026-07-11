<?php

declare(strict_types=1);

namespace Flow\Floe\Decoding;

use Flow\Floe\Format;

use function ord;

final class OptionalDecoder implements ValueDecoder
{
    public function __construct(
        private readonly ValueDecoder $base,
    ) {}

    public function decode(string $data, int &$position): mixed
    {
        if (ord($data[$position++]) === Format::VALUE_NULL) {
            return null;
        }

        return $this->base->decode($data, $position);
    }
}
