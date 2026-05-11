<?php

declare(strict_types=1);

namespace Flow\Arrow;

if (\extension_loaded('arrow')) {
    return;
}

interface OutputStream
{
    public function append(string $data): self;
}
