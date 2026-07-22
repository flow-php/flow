<?php

declare(strict_types=1);

namespace Flow\Floe;

final readonly class FooterLocation
{
    public function __construct(
        public Footer $footer,
        public int $footerFrameStart,
    ) {}
}
