<?php

declare(strict_types=1);

namespace Flow\Arrow;

use function extension_loaded;

if (extension_loaded('arrow')) {
    return;
}

interface OutputStream
{
    public function append(string $data): self;
}
