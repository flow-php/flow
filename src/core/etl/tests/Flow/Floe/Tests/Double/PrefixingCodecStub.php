<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Double;

use Flow\Floe\Codec;

use function substr;

/**
 * Codec double that visibly transforms the payload, unlike the identity CodecStub.
 */
final class PrefixingCodecStub implements Codec
{
    public function __construct(
        private readonly int $id = 0x00,
    ) {}

    public function decode(string $data): string
    {
        return substr($data, 1);
    }

    public function encode(string $data): string
    {
        return "\xFF" . $data;
    }

    public function id(): int
    {
        return $this->id;
    }
}
