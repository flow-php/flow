<?php

declare(strict_types=1);

namespace Flow\Floe;

/**
 * Compression codec contract for ROW frame bodies. Format v1 supports only the
 * no-op codec (id 0x00); the codec id is stamped into the header flags byte.
 */
interface Codec
{
    public function decode(string $data): string;

    public function encode(string $data): string;

    public function id(): int;
}
