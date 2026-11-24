<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Schema;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Foundation\Hash\HashAlgorithm;
use Flow\ETL\Values\Schema;
use JsonException;

use function hash;
use function ini_get;
use function ini_set;
use function json_encode;
use function sprintf;

use const JSON_PRESERVE_ZERO_FRACTION;
use const JSON_THROW_ON_ERROR;
use const JSON_UNESCAPED_SLASHES;
use const JSON_UNESCAPED_UNICODE;

final readonly class Fingerprint
{
    private const string VERSION = 'v1';

    private const int JSON_FLAGS = JSON_PRESERVE_ZERO_FRACTION | JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES;

    private string $canonical;

    public function __construct(Schema $schema, FingerprintOptions $options = new FingerprintOptions())
    {
        $columns = [];

        foreach ($schema->definitions() as $definition) {
            $column = $definition->normalize();
            unset($column['metadata']);

            if ($options->metadataKeys !== []) {
                $column['metadata'] = $this->selectedMetadata($definition->metadata(), $options->metadataKeys);
            }

            $columns[] = $column;
        }

        $canonical = [
            'version' => self::VERSION,
            'options' => ['metadata_keys' => $options->metadataKeys],
            'columns' => $columns,
        ];

        $this->canonical = $options->metadataKeys === []
            ? $this->encode($canonical)
            : $this->encodeWithShortestFloats($canonical);
    }

    public function canonical(): string
    {
        return $this->canonical;
    }

    public function equals(self $other): bool
    {
        return $this->canonical === $other->canonical;
    }

    public function hash(HashAlgorithm $algorithm = HashAlgorithm::xxh128): string
    {
        return hash($algorithm->value, $this->canonical);
    }

    public function toString(): string
    {
        return $this->hash();
    }

    /**
     * @param list<string> $keys
     *
     * @return array<array-key, null|array<array-key, mixed>|bool|float|int|string>
     */
    private function selectedMetadata(Metadata $metadata, array $keys): array
    {
        $selected = [];

        foreach ($keys as $key) {
            $selected[$key] = $metadata->has($key) ? $metadata->get($key) : null;
        }

        return $selected;
    }

    /**
     * @param array{version: string, options: array<string, mixed>, columns: list<array<string, mixed>>} $canonical
     */
    private function encodeWithShortestFloats(array $canonical): string
    {
        $precision = (string) ini_get('serialize_precision');
        // json_encode() formats floats with serialize_precision; -1 is the shortest round-trip form.
        ini_set('serialize_precision', '-1');

        try {
            if (ini_get('serialize_precision') !== '-1') {
                throw new RuntimeException(sprintf(
                    'Schema fingerprint with metadata keys needs serialize_precision=-1 to encode floats the same way '
                    . 'on every host, but the host locks it at "%s" (php_admin_value). Remove that setting, or '
                    . 'fingerprint without metadata keys.',
                    (string) ini_get('serialize_precision'),
                ));
            }

            return $this->encode($canonical);
        } finally {
            ini_set('serialize_precision', $precision);
        }
    }

    /**
     * @param array{version: string, options: array<string, mixed>, columns: list<array<string, mixed>>} $canonical
     */
    private function encode(array $canonical): string
    {
        try {
            return json_encode($canonical, JSON_THROW_ON_ERROR | self::JSON_FLAGS);
        } catch (JsonException $exception) {
            try {
                json_encode($canonical['options'], JSON_THROW_ON_ERROR | self::JSON_FLAGS);
            } catch (JsonException $keysException) {
                throw new InvalidArgumentException(
                    sprintf(
                        'Schema fingerprint cannot encode the selected metadata keys: %s. Pass valid UTF-8 keys to '
                        . 'withMetadataKeys().',
                        $keysException->getMessage(),
                    ),
                    previous: $keysException,
                );
            }

            foreach ($canonical['columns'] as $column) {
                try {
                    json_encode($column, JSON_THROW_ON_ERROR | self::JSON_FLAGS);
                } catch (JsonException $columnException) {
                    throw new InvalidArgumentException(
                        sprintf(
                            'Schema fingerprint cannot encode column "%s": %s. Column names and selected metadata values '
                            . 'must be valid UTF-8, and selected metadata floats must be finite (no NAN or INF).',
                            (string) $column['ref'],
                            $columnException->getMessage(),
                        ),
                        previous: $columnException,
                    );
                }
            }

            throw $exception;
        }
    }
}
