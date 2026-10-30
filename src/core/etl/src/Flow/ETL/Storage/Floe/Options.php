<?php

declare(strict_types=1);

namespace Flow\ETL\Storage\Floe;

use Flow\ETL\Values\Column\Frame\Codec;
use Flow\ETL\Values\Column\Frame\NoopCodec;

final readonly class Options
{
    public function __construct(
        public int $bufferSize = 65_536,
        public Codec $codec = new NoopCodec(),
    ) {}

    public static function default(): self
    {
        return new self();
    }

    public function withBufferSize(int $bufferSize): self
    {
        return new self($bufferSize, $this->codec);
    }

    public function withCodec(Codec $codec): self
    {
        return new self($this->bufferSize, $codec);
    }
}
