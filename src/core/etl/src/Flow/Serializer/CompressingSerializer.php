<?php

declare(strict_types=1);

namespace Flow\Serializer;

use Flow\ETL\Exception\RuntimeException;

final readonly class CompressingSerializer implements Serializer
{
    public function __construct(private Serializer $serializer, private int $compressionLevel = 9)
    {
    }

    public function serialize(object $serializable) : string
    {
        if (!\function_exists('gzcompress')) {
            // @codeCoverageIgnoreStart
            throw new RuntimeException("'ext-zlib' is missing in, compression impossible due to lack of gzcompress.");
            // @codeCoverageIgnoreEnd
        }

        $content = \gzcompress($this->serializer->serialize($serializable), $this->compressionLevel);

        if (false === $content) {
            // @codeCoverageIgnoreStart
            throw new \RuntimeException('Unable to compress serialized data.');
            // @codeCoverageIgnoreEnd
        }

        return $content;
    }

    public function unserialize(string $serialized, array $classes) : object
    {
        if (!\function_exists('gzcompress')) {
            // @codeCoverageIgnoreStart
            throw new RuntimeException("'ext-zlib' is missing in, decompression impossible due to lack of gzuncompress.");
            // @codeCoverageIgnoreEnd
        }

        $content = \gzuncompress($serialized);

        if (false === $content) {
            // @codeCoverageIgnoreStart
            throw new \RuntimeException('Unable to decompress unserialized data.');
            // @codeCoverageIgnoreEnd
        }

        return $this->serializer->unserialize($content, $classes);
    }
}
