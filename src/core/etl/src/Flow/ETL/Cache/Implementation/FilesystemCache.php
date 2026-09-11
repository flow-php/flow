<?php

declare(strict_types=1);

namespace Flow\ETL\Cache\Implementation;

use Flow\ETL\Cache;
use Flow\ETL\Exception\KeyNotInCacheException;
use Flow\ETL\Hash\NativePHPHash;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;
use Flow\Floe\FloeSerializer;
use Flow\Serializer\Exception\SerializationException;
use Flow\Serializer\Serializer;
use JsonException;

use function Flow\ETL\DSL\schema_from_json;
use function Flow\ETL\DSL\schema_to_json;
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
        $this->filesystem->rm($this->schemaPath($key));
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

    public function schema(string $key): Schema
    {
        $path = $this->schemaPath($key);

        if (!$this->filesystem->status($path)) {
            throw new KeyNotInCacheException($key);
        }

        $stream = $this->filesystem->readFrom($path);

        try {
            return schema_from_json($stream->content());
        } catch (JsonException $e) {
            throw new KeyNotInCacheException($key, $e);
        } finally {
            $stream->close();
        }
    }

    public function set(string $key, Rows $value): void
    {
        $this->serializer->serialize($value, $this->filesystem->writeTo($this->cachePath($key)));

        $schemaStream = $this->filesystem->writeTo($this->schemaPath($key));
        $schemaStream->append(schema_to_json($value->schema()));
        $schemaStream->close();
    }

    private function cachePath(string $key): Path
    {
        return $this->cacheDir->suffix(
            implode('/', str_split(substr(NativePHPHash::xxh128($key), 0, 8), 2)) . '/' . $key,
        );
    }

    private function schemaPath(string $key): Path
    {
        return $this->cachePath($key . '.schema');
    }
}
