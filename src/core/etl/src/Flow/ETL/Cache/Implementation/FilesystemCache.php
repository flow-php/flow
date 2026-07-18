<?php

declare(strict_types=1);

namespace Flow\ETL\Cache\Implementation;

use Flow\ETL\Cache;
use Flow\ETL\Exception\KeyNotInCacheException;
use Flow\ETL\Hash\NativePHPHash;
use Flow\ETL\Rows;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;
use Flow\Floe\FloeSerializer;
use Flow\Serializer\Exception\SerializationException;
use Flow\Serializer\Serializer;

use function str_split;
use function substr;

final readonly class FilesystemCache implements Cache
{
    private Path $cacheDir;

    public function __construct(
        private Filesystem $filesystem,
        ?Path $cacheDir = null,
        private Serializer $serializer = new FloeSerializer(),
    ) {
        $this->cacheDir = $cacheDir ?? $this->filesystem->getSystemTmpDir();
    }

    public function clear(): void
    {
        $this->filesystem->rm($this->cacheDir);
    }

    public function delete(string $key): void
    {
        $this->filesystem->rm($this->cachePath($key));
    }

    public function get(string $key): Rows
    {
        $path = $this->cachePath($key);

        if (!$this->filesystem->status($path)) {
            throw new KeyNotInCacheException($key);
        }

        try {
            return $this->serializer->unserialize($this->filesystem->readFrom($path));
        } catch (SerializationException $e) {
            throw new KeyNotInCacheException($key, $e);
        }
    }

    public function has(string $key): bool
    {
        return $this->filesystem->status($this->cachePath($key)) !== null;
    }

    public function set(string $key, Rows $value): void
    {
        $this->serializer->serialize($value, $this->filesystem->writeTo($this->cachePath($key)));
    }

    private function cachePath(string $key): Path
    {
        return $this->cacheDir->suffix(
            implode('/', str_split(substr(NativePHPHash::xxh128($key), 0, 8), 2)) . '/' . $key,
        );
    }
}
