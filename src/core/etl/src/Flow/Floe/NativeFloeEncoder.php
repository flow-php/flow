<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Row\Encoder;
use Flow\ETL\Schema;
use Flow\Floe\Exception\ExtensionException;
use Flow\Floe\Exception\FloeException;
use JsonException;
use RuntimeException;

use function class_exists;
use function extension_loaded;
use function json_encode;

use const JSON_THROW_ON_ERROR;

/**
 * @implements Encoder<string>
 */
final class NativeFloeEncoder implements Encoder
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
        return extension_loaded('flow_php') && class_exists(RustFloeEncoderNative::class, false);
    }

    public function decode(array $batch): array
    {
        try {
            return $this->native->decode($batch, $this->schemaBody());
        } catch (ExtensionException $e) {
            throw new FloeException($e->getMessage(), 0, $e);
        }
    }

    public function encode(array $batch): array
    {
        try {
            return $this->native->encode($batch, $this->schemaBody());
        } catch (ExtensionException $e) {
            throw new FloeException($e->getMessage(), 0, $e);
        }
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
