<?php

declare(strict_types=1);

namespace Flow\ETL\Storage;

use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Storage\Serializer\Serializer;
use Flow\ETL\Values\Cardinality;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\Filesystem\Path;
use Flow\Filesystem\Stream\StringDestinationStream;
use Flow\Filesystem\Stream\StringSourceStream;
use Generator;

use function apcu_delete;
use function apcu_enabled;
use function apcu_exists;
use function apcu_fetch;
use function apcu_store;
use function extension_loaded;
use function is_array;
use function is_int;
use function is_string;
use function sprintf;

/**
 * @type Entry = array{chunks: int, rows: int, schema: array<array-key, mixed>}
 */
final readonly class ApcuRowsStorage implements RowsStorage
{
    public function __construct(
        private Serializer $serializer,
        private string $namespace = 'flow_php_cache',
    ) {
        if (!extension_loaded('apcu') || !apcu_enabled()) {
            throw new RuntimeException(
                'ApcuCache requires the APCu extension (ext-apcu) with apc.enabled=1'
                . (PHP_SAPI === 'cli' ? ' and apc.enable_cli=1' : ''),
            );
        }
    }

    public function append(string $key, Rows $rows): void
    {
        $entry = $this->entry($key);
        $destination = new StringDestinationStream(Path::from('memory://rows'));
        $this->serializer->serialize($rows, $destination);

        if (
            apcu_store($this->chunkKey($key, $entry['chunks']), $destination->content()) === false
            || apcu_store($this->entryKey($key), [
                'chunks' => $entry['chunks'] + 1,
                'rows' => $entry['rows'] + $rows->count(),
                'schema' => Schema::fromArray($entry['schema'])->merge($rows->schema())->normalize(),
            ]) === false
        ) {
            throw new RuntimeException(sprintf(
                'Failed to store rows under "%s" in APCu, the cache segment might be full.',
                $key,
            ));
        }
    }

    public function close(string $key): void {}

    public function get(string $key): Generator
    {
        for ($i = 0, $chunks = $this->entry($key)['chunks']; $i < $chunks; $i++) {
            // @mago-ignore analysis:mixed-assignment
            $payload = apcu_fetch($this->chunkKey($key, $i));

            if (!is_string($payload)) {
                throw new RuntimeException(sprintf(
                    'Rows under "%s" are corrupted or were not written by ApcuCache.',
                    $key,
                ));
            }

            yield $this->serializer->unserialize(new StringSourceStream(Path::from('memory://rows'), $payload));
        }
    }

    public function has(string $key): bool
    {
        return (bool) apcu_exists($this->entryKey($key));
    }

    public function remove(string $key): void
    {
        $keys = [$this->entryKey($key)];

        for ($i = 0, $chunks = $this->entry($key)['chunks']; $i < $chunks; $i++) {
            $keys[] = $this->chunkKey($key, $i);
        }

        apcu_delete($keys);
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
        return $this->namespace . ':' . $key . ':chunk:' . $n;
    }

    /**
     * @return Entry
     */
    private function entry(string $key): array
    {
        // @mago-ignore analysis:mixed-assignment
        $entry = apcu_fetch($this->entryKey($key));

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
        return $this->namespace . ':' . $key;
    }
}
