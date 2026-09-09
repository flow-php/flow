<?php

declare(strict_types=1);

namespace Flow\Documentation\Tests\Context;

use Flow\Filesystem\Filesystem;

use function Flow\Filesystem\DSL\fstab;
use function Flow\Filesystem\DSL\path;
use function sys_get_temp_dir;
use function uniqid;

final class TempFileContext
{
    /** @var list<string> */
    private array $written = [];

    private readonly Filesystem $filesystem;

    public function __construct(
        private readonly string $extension,
    ) {
        $this->filesystem = fstab()->for('file');
    }

    public function clean(): void
    {
        foreach ($this->written as $file) {
            $this->filesystem->rm(path($file));
        }

        $this->written = [];
    }

    public function withContents(string $contents): string
    {
        $file = sys_get_temp_dir() . '/flow-documentation-' . uniqid() . '.' . $this->extension;
        $stream = $this->filesystem->writeTo(path($file));
        $stream->append($contents);
        $stream->close();
        $this->written[] = $file;

        return $file;
    }
}
