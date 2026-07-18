<?php

declare(strict_types=1);

namespace Flow\Serializer;

use Flow\ETL\Rows;
use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\SourceStream;
use Flow\Serializer\Exception\SerializationException;

use function Flow\Serializer\DSL\serialize_to_string;
use function Flow\Serializer\DSL\unserialize_from_string;
use function function_exists;
use function gzcompress;
use function gzuncompress;

final readonly class CompressingSerializer implements Serializer
{
    public function __construct(
        private Serializer $serializer,
        private int $compressionLevel = 9,
    ) {}

    public function serialize(Rows $rows, DestinationStream $destination): void
    {
        if (!function_exists('gzcompress')) {
            // @codeCoverageIgnoreStart
            throw new SerializationException(
                "'ext-zlib' is missing in, compression impossible due to lack of gzcompress.",
            );

            // @codeCoverageIgnoreEnd
        }

        $content = gzcompress(serialize_to_string($this->serializer, $rows), $this->compressionLevel);

        if (false === $content) {
            // @codeCoverageIgnoreStart
            throw new SerializationException('Unable to compress serialized data.');

            // @codeCoverageIgnoreEnd
        }

        $destination->append($content);
        $destination->close();
    }

    public function unserialize(SourceStream $source): Rows
    {
        if (!function_exists('gzcompress')) {
            // @codeCoverageIgnoreStart
            throw new SerializationException(
                "'ext-zlib' is missing in, decompression impossible due to lack of gzuncompress.",
            );

            // @codeCoverageIgnoreEnd
        }

        $payload = $source->content();
        $source->close();

        $content = gzuncompress($payload);

        if (false === $content) {
            // @codeCoverageIgnoreStart
            throw new SerializationException('Unable to decompress unserialized data.');

            // @codeCoverageIgnoreEnd
        }

        return unserialize_from_string($this->serializer, $content);
    }
}
