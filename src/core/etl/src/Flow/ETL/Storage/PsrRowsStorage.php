<?php

declare(strict_types=1);

namespace Flow\ETL\Storage;

use DateInterval;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Storage\Serializer\Serializer;
use Flow\ETL\Values\Cardinality;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\Filesystem\Path;
use Flow\Filesystem\Stream\StringDestinationStream;
use Flow\Filesystem\Stream\StringSourceStream;
use Generator;
use Psr\SimpleCache\CacheInterface;

use function is_array;
use function is_int;
use function is_string;
use function json_decode;
use function json_encode;
use function strpbrk;

use const JSON_THROW_ON_ERROR;

/**
 * @type Entry = array{chunks: int, rows: int, schema: array<array-key, mixed>}
 */
final readonly class PsrRowsStorage implements RowsStorage
{
    public function __construct(
        private CacheInterface $cache,
        private Serializer $serializer,
        private string $prefix = 'flow.rows',
        private int|DateInterval|null $ttl = null,
    ) {
        if (strpbrk($this->prefix, '{}()/\\@:') !== false) {
            throw new InvalidArgumentException('Prefix must not contain PSR-16 reserved characters {}()/\\@:, given: '
            . $this->prefix);
        }
    }

    public function append(string $key, Rows $rows): void
    {
        $entry = $this->entry($key);
        $destination = new StringDestinationStream(Path::from('memory://rows'));
        $this->serializer->serialize($rows, $destination);

        $this->cache->set($this->chunkKey($key, $entry['chunks']), $destination->content(), $this->ttl);
        $this->cache->set(
            $this->entryKey($key),
            json_encode([
                'chunks' => $entry['chunks'] + 1,
                'rows' => $entry['rows'] + $rows->count(),
                'schema' => Schema::fromArray($entry['schema'])->merge($rows->schema())->normalize(),
            ], JSON_THROW_ON_ERROR),
            $this->ttl,
        );
    }

    public function close(string $key): void {}

    /**
     * @return Generator<Rows>
     */
    public function get(string $key): Generator
    {
        for ($i = 0, $chunks = $this->entry($key)['chunks']; $i < $chunks; $i++) {
            // @mago-ignore analysis:mixed-assignment
            $payload = $this->cache->get($this->chunkKey($key, $i));

            if (!is_string($payload)) {
                continue;
            }

            yield $this->serializer->unserialize(new StringSourceStream(Path::from('memory://rows'), $payload));
        }
    }

    public function has(string $key): bool
    {
        return $this->cache->has($this->entryKey($key));
    }

    public function remove(string $key): void
    {
        $keys = [$this->entryKey($key)];

        for ($i = 0, $chunks = $this->entry($key)['chunks']; $i < $chunks; $i++) {
            $keys[] = $this->chunkKey($key, $i);
        }

        $this->cache->deleteMultiple($keys);
    }

    public function rows(string $key): Cardinality
    {
        return Cardinality::exact($this->entry($key)['rows']);
    }

    public function schema(string $key): ?Schema
    {
        return $this->has($key) ? Schema::fromArray($this->entry($key)['schema']) : null;
    }

    private function chunkKey(string $key, int $n): string
    {
        return $this->prefix . '.' . $key . '.chunk.' . $n;
    }

    /**
     * @return Entry
     */
    private function entry(string $key): array
    {
        // @mago-ignore analysis:mixed-assignment
        $json = $this->cache->get($this->entryKey($key));
        // @mago-ignore analysis:mixed-assignment
        $entry = is_string($json) ? json_decode($json, true) : null;

        if (
            !is_array($entry)
            || !is_int($entry['chunks'] ?? null)
            || !is_int($entry['rows'] ?? null)
            || !is_array($entry['schema'] ?? null)
        ) {
            return ['chunks' => 0, 'rows' => 0, 'schema' => []];
        }

        return ['chunks' => $entry['chunks'], 'rows' => $entry['rows'], 'schema' => $entry['schema']];
    }

    private function entryKey(string $key): string
    {
        return $this->prefix . '.' . $key . '.entry';
    }
}
