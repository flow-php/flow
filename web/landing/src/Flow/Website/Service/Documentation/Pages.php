<?php

declare(strict_types=1);

namespace Flow\Website\Service\Documentation;

use Flow\Website\Model\Documentation\Page;
use RuntimeException;

use function file_exists;
use function file_get_contents;
use function Flow\Filesystem\DSL\fstab;
use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\type_string;
use function realpath;
use function rtrim;
use function str_ends_with;
use function str_replace;
use function str_starts_with;

final readonly class Pages
{
    public function __construct(
        private string $basePath,
    ) {}

    /**
     * @return array<Page>
     */
    public function all(): array
    {
        $files = fstab()->for('file')->list(path($this->basePath . '/**/*.md'));

        $pages = [];

        foreach ($files as $file) {
            $relativePath = str_replace(
                type_string()->assert(realpath($this->basePath)) . '/',
                '',
                $file->path->path(),
            );

            if (str_starts_with($relativePath, '_')) {
                continue;
            }

            $relativePath = str_replace('.md', '', $relativePath);

            $pages[] = new Page($relativePath, type_string()->assert(file_get_contents($file->path->path())));
        }

        return $pages;
    }

    public function get(string $path): Page
    {
        $path = rtrim($path, '/');

        if (!str_ends_with($path, '.md')) {
            $path .= '.md';
        }

        if (file_exists($this->basePath . '/' . $path)) {
            return new Page($path, type_string()->assert(file_get_contents($this->basePath . '/' . $path)));
        }

        throw new RuntimeException('Page not found: ' . $path);
    }
}
