<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Datasets;

use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;
use function glob;

/**
 * Fingerprint lives in the filename so it cannot drift from the bytes.
 */
final readonly class FixturePath
{
    public function __construct(
        private string $prefix,
        private int $rows,
        private FixtureFormat $format,
    ) {}

    public function exists(): bool
    {
        return native_local_filesystem()->status(path($this->path())) !== null;
    }

    public function path(): string
    {
        return (
            Paths::datasets()
            . '/'
            . $this->prefix
            . '_'
            . $this->rows
            . '.'
            . (new FixtureFingerprint($this->format))->value()
            . '.'
            . $this->format->extension()
        );
    }

    public function prune(): void
    {
        $current = $this->path();
        $stem = Paths::datasets() . '/' . $this->prefix . '_' . $this->rows;
        $filesystem = native_local_filesystem();

        foreach ([
            ...(glob($stem . '.*.' . $this->format->extension()) ?: []),
            ...(glob($stem . '.' . $this->format->extension()) ?: []),
        ] as $sibling) {
            if ($sibling !== $current) {
                $filesystem->rm(path($sibling));
            }
        }
    }
}
