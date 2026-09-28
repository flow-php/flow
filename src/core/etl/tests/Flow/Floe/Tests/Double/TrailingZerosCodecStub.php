<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Double;

use Flow\Floe\Codec;

use function chr;
use function ord;
use function rtrim;
use function str_repeat;
use function strlen;
use function substr;

/**
 * Codec double that shrinks a buffer ending in zero bytes, so the per-buffer length prefix is exercised.
 */
final class TrailingZerosCodecStub implements Codec
{
    public function decode(string $data): string
    {
        return substr($data, 0, -1) . str_repeat("\0", ord($data[strlen($data) - 1]));
    }

    public function encode(string $data): string
    {
        $trimmed = rtrim($data, "\0");

        return $trimmed . chr(strlen($data) - strlen($trimmed));
    }

    public function id(): int
    {
        return 0x00;
    }
}
