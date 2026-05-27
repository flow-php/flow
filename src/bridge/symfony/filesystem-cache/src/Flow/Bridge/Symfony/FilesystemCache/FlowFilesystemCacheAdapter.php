<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemCache;

use Flow\Bridge\Symfony\FilesystemCache\Exception\FilesystemCacheException;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use Symfony\Component\Cache\Adapter\AbstractAdapter;
use Symfony\Component\Cache\Exception\InvalidArgumentException;
use Symfony\Component\Cache\Marshaller\DefaultMarshaller;
use Symfony\Component\Cache\Marshaller\MarshallerInterface;
use Symfony\Component\Cache\PruneableInterface;

use function base64_encode;
use function count;
use function explode;
use function hash;
use function is_string;
use function preg_match;
use function rtrim;
use function sprintf;
use function str_replace;
use function str_starts_with;
use function substr;
use function time;

final class FlowFilesystemCacheAdapter extends AbstractAdapter implements PruneableInterface
{
    private const int MAX_KEY_LENGTH = 255;

    private readonly MarshallerInterface $marshaller;

    public function __construct(
        private readonly Filesystem $filesystem,
        private readonly Path $directory,
        string $namespace = '',
        int $defaultLifetime = 0,
        ?MarshallerInterface $marshaller = null,
    ) {
        $match = [];

        if (isset($namespace[0]) && preg_match('#[^-+.A-Za-z0-9]#', $namespace, $match)) {
            throw new InvalidArgumentException(sprintf(
                'Namespace contains "%s" but only characters in [-+.A-Za-z0-9] are allowed.',
                $match[0],
            ));
        }
        $this->marshaller = $marshaller ?? new DefaultMarshaller();
        $this->maxIdLength = self::MAX_KEY_LENGTH;

        parent::__construct($namespace, $defaultLifetime);
    }

    public function prune(): bool
    {
        $now = time();

        foreach ($this->filesystem->list($this->listPattern(), new OnlyFiles()) as $status) {
            $expiry = $this->readExpiry($status->path);

            if ($expiry > 0 && $expiry <= $now) {
                $this->filesystem->rm($status->path);
            }
        }

        return true;
    }

    protected function doClear(string $namespace): bool
    {
        foreach ($this->filesystem->list($this->listPattern(), new OnlyFiles()) as $status) {
            if ($namespace === '') {
                $this->filesystem->rm($status->path);

                continue;
            }

            if (str_starts_with($this->readId($status->path), $namespace)) {
                $this->filesystem->rm($status->path);
            }
        }

        return true;
    }

    /**
     * @param array<array-key, mixed> $ids
     */
    protected function doDelete(array $ids): bool
    {
        $ok = true;

        // @mago-expect analysis:mixed-assignment
        foreach ($ids as $id) {
            $path = $this->fileFor((string) $id);

            if ($this->filesystem->status($path) === null) {
                continue;
            }

            $ok = $this->filesystem->rm($path) && $ok;
        }

        return $ok;
    }

    /**
     * @param array<array-key, mixed> $ids
     *
     * @return iterable<string, mixed>
     */
    protected function doFetch(array $ids): iterable
    {
        $now = time();
        $expired = [];

        // @mago-expect analysis:mixed-assignment
        foreach ($ids as $id) {
            $path = $this->fileFor((string) $id);

            if ($this->filesystem->status($path) === null) {
                continue;
            }

            $stream = $this->filesystem->readFrom($path);

            try {
                $content = $stream->content();
            } finally {
                $stream->close();
            }

            $parts = explode("\n", $content, 3);

            if (count($parts) < 3) {
                throw FilesystemCacheException::corruptedCacheFile($path->path());
            }

            [$expiry, $key, $value] = $parts;
            $expiryTime = (int) $expiry;

            if ($expiryTime !== 0 && $expiryTime <= $now) {
                $expired[] = $id;

                continue;
            }

            yield $key => $this->marshaller->unmarshall($value);
        }

        if ($expired !== []) {
            $this->doDelete($expired);
        }
    }

    protected function doHave(string $id): bool
    {
        $path = $this->fileFor($id);

        if ($this->filesystem->status($path) === null) {
            return false;
        }

        $expiry = $this->readExpiry($path);

        return $expiry === 0 || $expiry > time();
    }

    /**
     * @param array<array-key, mixed> $values
     *
     * @return array<int, string>
     */
    protected function doSave(array $values, int $lifetime): array
    {
        $failed = [];
        $marshalled = $this->marshaller->marshall($values, $failed);
        $failedKeys = $this->collectFailedKeys($failed);

        if ($marshalled === []) {
            return $failedKeys;
        }

        $expiry = $lifetime > 0 ? time() + $lifetime : 0;

        // @mago-expect analysis:mixed-assignment
        foreach ($marshalled as $id => $value) {
            $path = $this->fileFor((string) $id);
            $tmp = $path->randomize();
            $content = sprintf('%010d', $expiry) . "\n" . (string) $id . "\n" . (string) $value;

            $stream = $this->filesystem->writeTo($tmp);

            try {
                $stream->append($content);
            } finally {
                $stream->close();
            }

            if (!$this->filesystem->mv($tmp, $path)) {
                $this->filesystem->rm($tmp);

                throw FilesystemCacheException::writeFailed($path->path(), 'mv returned false');
            }
        }

        return $failedKeys;
    }

    /**
     * @param array<array-key, mixed>|null $failed
     *
     * @return list<string>
     */
    private function collectFailedKeys(?array $failed): array
    {
        $result = [];

        // @mago-expect analysis:mixed-assignment
        foreach ($failed ?? [] as $key) {
            if (is_string($key)) {
                $result[] = $key;
            }
        }

        return $result;
    }

    private function fileFor(string $id): Path
    {
        $hash = str_replace('/', '-', base64_encode(hash('xxh128', $id, true)));

        return Path::from(rtrim($this->directory->uri(), '/') . '/' . substr($hash, 0, 2) . '/' . substr($hash, 2));
    }

    private function listPattern(): Path
    {
        return Path::from(rtrim($this->directory->uri(), '/') . '/**/*');
    }

    private function readExpiry(Path $path): int
    {
        $stream = $this->filesystem->readFrom($path);

        try {
            foreach ($stream->readLines() as $line) {
                return (int) $line;
            }
        } finally {
            $stream->close();
        }

        return 0;
    }

    private function readId(Path $path): string
    {
        $stream = $this->filesystem->readFrom($path);

        try {
            $index = 0;

            foreach ($stream->readLines() as $line) {
                if ($index === 1) {
                    return $line;
                }

                $index++;
            }
        } finally {
            $stream->close();
        }

        return '';
    }
}
