<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\Floe\Codec\NoopCodec;

final readonly class Options
{
    /**
     * @param bool $validateData gates the per-value type check on write. The column-set check -
     *                           a row carrying a column the session schema does not declare -
     *                           always runs, so silent column loss cannot be unlocked. Mirrors
     *                           parquet-java's ParquetWriter::withValidation().
     */
    public function __construct(
        public bool $validateData = true,
        public int $bufferSize = 65_536,
        public Codec $codec = new NoopCodec(),
    ) {}

    public static function default(): self
    {
        return new self();
    }

    public function withBufferSize(int $bufferSize): self
    {
        return new self($this->validateData, $bufferSize, $this->codec);
    }

    public function withCodec(Codec $codec): self
    {
        return new self($this->validateData, $this->bufferSize, $codec);
    }

    public function withValidateData(bool $validateData): self
    {
        return new self($validateData, $this->bufferSize, $this->codec);
    }
}
