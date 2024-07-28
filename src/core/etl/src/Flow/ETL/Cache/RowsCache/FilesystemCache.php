<?php

declare(strict_types=1);

namespace Flow\ETL\Cache\RowsCache;

use Flow\ETL\Exception\{RuntimeException};
use Flow\ETL\{Cache\RowsCache, Hash\NativePHPHash, Rows};
use Flow\Filesystem\{Filesystem, Path};
use Flow\Serializer\Serializer;

/**
 * @infection-ignore-all
 */
final class FilesystemCache implements RowsCache
{
    private Path $cacheDir;

    public function __construct(
        private readonly Filesystem $filesystem,
        private readonly Serializer $serializer,
        ?Path $cacheDir,
    ) {
        $this->cacheDir = ($cacheDir ?? $this->filesystem->getSystemTmpDir())->suffix('/rows/');
    }

    public function append(string $key, Rows $rows) : void
    {
        $cacheStream = $this->filesystem->appendTo($this->cachePath($key));
        $cacheStream->append($this->serializer->serialize($rows) . "\n");
        $cacheStream->close();
    }

    /**
     * @throws RuntimeException
     *
     * @return \Generator<Rows>
     */
    public function get(string $key) : \Generator
    {
        if (!$this->has($key)) {
            return;
        }

        $stream = $this->filesystem->readFrom($this->cachePath($key));

        foreach ($stream->readLines() as $serializedRow) {
            /** @var Rows $rows */
            $rows = $this->serializer->unserialize($serializedRow, Rows::class);

            yield $rows;
        }

        $stream->close();
    }

    public function has(string $key) : bool
    {
        return $this->filesystem->status($this->cachePath($key)) !== null;
    }

    public function remove(string $key) : void
    {
        $this->filesystem->rm($this->cachePath($key));
    }

    private function cachePath(string $key) : Path
    {
        return $this->cacheDir->suffix(implode('/', \str_split(\substr(NativePHPHash::xxh128($key), 0, 8), 2)) . '/' . $key . '.php.cache');
    }
}
