<?php

declare(strict_types=1);

namespace Flow\Website\Service\Documentation;

use function Flow\Filesystem\DSL\{fstab, path, protocol};
use Flow\Website\Model\Documentation\{Page};

final class Pages
{
    public function __construct(
        private readonly string $basePath,
    ) {
    }

    /**
     * @return array<Page>
     */
    public function all() : array
    {
        $files = fstab()
            ->for(protocol('file'))
            ->list(
                path($this->basePath . '/**/*.md')
            );

        $pages = [];

        foreach ($files as $file) {
            $relativePath = \str_replace(\realpath($this->basePath) . '/', '', $file->path->path());

            if (\str_starts_with($relativePath, '_')) {
                continue;
            }

            $relativePath = str_replace('.md', '', $relativePath);

            $pages[] = new Page($relativePath, \file_get_contents($file->path->path()));
        }

        return $pages;
    }

    public function get(string $path) : Page
    {
        $path = \rtrim($path, '/');

        if (!\str_ends_with($path, '.md')) {
            $path .= '.md';
        }

        if (\file_exists($this->basePath . '/' . $path)) {
            return new Page($path, \file_get_contents($this->basePath . '/' . $path));
        }

        throw new \RuntimeException('Page not found: ' . $path);
    }
}
