<?php

declare(strict_types=1);

namespace Flow\Floe\Decoding;

interface ValueDecoder
{
    /**
     * @param int $position read position, advanced past the consumed bytes
     */
    public function decode(string $data, int &$position): mixed;
}
