<?php

declare(strict_types=1);

namespace Flow\Website\Service\Documentation;

use Flow\Website\Model\Documentation\{Page};

final class Pages
{
    public function __construct(
        private readonly string $basePath,
    ) {
    }

    public function get(string $path) : Page
    {
        $path = \rtrim($path, '/');

        if (!\str_ends_with($path, '.md')) {
            $path .= '.md';
        }

        if (\file_exists($this->basePath . '/' . $path)) {
            return new Page(\file_get_contents($this->basePath . '/' . $path));
        }

        throw new \RuntimeException('Page not found: ' . $path);
    }
}
