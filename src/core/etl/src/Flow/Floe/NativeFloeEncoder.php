<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Row\AdaptiveRowHydrator;
use Flow\ETL\Row\Hydrator;
use Flow\ETL\Row\NativeRowHydrator;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Floe\Exception\ExtensionException;
use Flow\Floe\Exception\FloeException;
use JsonException;
use RuntimeException;

use function class_exists;
use function extension_loaded;
use function json_encode;
use function method_exists;

use const JSON_THROW_ON_ERROR;

final class NativeFloeEncoder implements FloeEncoder
{
    private readonly RustFloeEncoderNative $native;

    private ?string $schemaBody = null;

    public function __construct(
        private readonly Schema $schema,
    ) {
        if (!self::isSupported()) {
            throw new RuntimeException('flow_php extension with RustFloeEncoderNative is not loaded');
        }

        $this->native = new RustFloeEncoderNative();
    }

    public static function isSupported(): bool
    {
        // an extension older than this library lacks decodeRows()/encodeFrames()/datetimeZones() - it falls back to the PHP engine instead
        return (
            extension_loaded('flow_php')
            && class_exists(RustFloeEncoderNative::class, false)
            && method_exists(RustFloeEncoderNative::class, 'decodeRows')
            && method_exists(RustFloeEncoderNative::class, 'encodeFrames')
            && method_exists(RustFloeEncoderNative::class, 'datetimeZones')
        );
    }

    public function decode(array $batch): array
    {
        try {
            return $this->native->decode($batch, $this->schemaBody());
        } catch (ExtensionException $e) {
            throw new FloeException($e->getMessage(), 0, $e);
        }
    }

    public function decodeRows(array $bodies, Schema $schema, Hydrator $hydrator): Rows
    {
        if (!self::isNativeHydrator($hydrator)) {
            return $hydrator->hydrate($this->decode($bodies), $schema);
        }

        try {
            return $this->native->decodeRows($bodies, $this->schemaBody(), $schema);
        } catch (ExtensionException $e) {
            throw new FloeException($e->getMessage(), 0, $e);
        }
    }

    public function encodeFrames(Rows $rows, Hydrator $hydrator): string
    {
        if (!self::isNativeHydrator($hydrator)) {
            return Format::rowFrames($this->encode($hydrator->dehydrate($rows)));
        }

        try {
            return $this->native->encodeFrames($rows, $this->schemaBody(), $this->schema);
        } catch (ExtensionException $e) {
            throw new FloeException($e->getMessage(), 0, $e);
        }
    }

    public function encode(array $batch): array
    {
        try {
            return $this->native->encode($batch, $this->schemaBody(), $this->schema);
        } catch (ExtensionException $e) {
            throw new FloeException($e->getMessage(), 0, $e);
        }
    }

    private static function isNativeHydrator(Hydrator $hydrator): bool
    {
        return (
            $hydrator instanceof NativeRowHydrator
            || $hydrator instanceof AdaptiveRowHydrator
            && $hydrator->isNative()
        );
    }

    private function schemaBody(): string
    {
        if ($this->schemaBody !== null) {
            return $this->schemaBody;
        }

        try {
            return $this->schemaBody = json_encode($this->schema->normalize(), JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            throw new FloeException('Floe failed to encode schema as JSON: ' . $e->getMessage(), 0, $e);
        }
    }
}
