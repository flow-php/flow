<?php

declare(strict_types=1);

namespace Flow\Floe\Codec;

use Flow\Floe\Codec;

final class NoopCodec implements Codec
{
    public function decode(string $data): string
    {
        return $data;
    }

    public function encode(string $data): string
    {
        return $data;
    }

    public function id(): int
    {
        return 0x00;
    }
}
