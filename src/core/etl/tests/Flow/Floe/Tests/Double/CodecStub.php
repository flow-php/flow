<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Double;

use Flow\Floe\Codec;

final class CodecStub implements Codec
{
    public function __construct(
        private readonly int $id,
    ) {}

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
        return $this->id;
    }
}
