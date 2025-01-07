<?php

declare(strict_types=1);

namespace Flow\ETL\Cache\Implementation;

use Flow\ETL\Cache\{CacheIndex};
use Flow\ETL\Exception\KeyNotInCacheException;
use Flow\ETL\Hash\NativePHPHash;
use Flow\ETL\{Cache, Row, Rows};
use Flow\Filesystem\{Filesystem, Path};
use Flow\Serializer\Serializer;

final readonly class FilesystemCache implements Cache
{
    private Path $cacheDir;

    public function __construct(
        private Filesystem $filesystem,
        private Serializer $serializer,
        ?Path $cacheDir,
    ) {
        $this->cacheDir = $cacheDir ?? $this->filesystem->getSystemTmpDir();
    }

    public function clear() : void
    {
        $this->filesystem->rm($this->cacheDir);
    }

    public function delete(string $key) : void
    {
        $this->filesystem->rm($this->cachePath($key));
    }

    public function get(string $key) : Row|Rows|CacheIndex
    {
        $path = $this->cachePath($key);

        if (!$this->filesystem->status($path)) {
            throw new KeyNotInCacheException($key);
        }

        $stream = $this->filesystem->readFrom($path);

        $serializedValue = $stream->content();
        $stream->close();

        return $this->serializer->unserialize($serializedValue, [Row::class, Rows::class, CacheIndex::class]);
    }

    public function has(string $key) : bool
    {
        return $this->filesystem->status($this->cachePath($key)) !== null;
    }

    public function set(string $key, CacheIndex|Rows|Row $value) : void
    {
        $cacheStream = $this->filesystem->writeTo($this->cachePath($key));
        $cacheStream->append($this->serializer->serialize($value));
        $cacheStream->close();
    }

    private function cachePath(string $key) : Path
    {
        return $this->cacheDir->suffix(implode('/', \str_split(\substr(NativePHPHash::xxh128($key), 0, 8), 2)) . '/' . $key . '.php.cache');
    }
}
