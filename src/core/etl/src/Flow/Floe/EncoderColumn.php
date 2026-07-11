<?php

declare(strict_types=1);

namespace Flow\Floe;

final readonly class EncoderColumn
{
    /**
     * @param string $typeFingerprint JSON of Type::normalize() - the section-fit discriminator
     */
    public function __construct(
        public string $name,
        public string $typeFingerprint,
        public Encoding\ValueEncoder $encoder,
    ) {}
}
