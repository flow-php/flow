<?php

declare(strict_types=1);

namespace Flow\Serializer;

use Flow\ETL\Exception\RuntimeException;

final class CompressingSerializer implements Serializer
{
    public function __construct(private readonly Serializer $serializer, private readonly int $compressionLevel = 9)
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

        return \base64_encode($content);
    }

    public function unserialize(string $serialized, string $class) : object
    {
        if (!\function_exists('gzcompress')) {
            // @codeCoverageIgnoreStart
            throw new RuntimeException("'ext-zlib' is missing in, decompression impossible due to lack of gzuncompress.");
            // @codeCoverageIgnoreEnd
        }

        $content = \base64_decode($serialized, true);

        if (false === $content) {
            // @codeCoverageIgnoreStart
            throw new \RuntimeException('Unable to decode serialized data.');
            // @codeCoverageIgnoreEnd
        }

        $content = \gzuncompress($content);

        if (false === $content) {
            // @codeCoverageIgnoreStart
            throw new \RuntimeException('Unable to decompress unserialized data.');
            // @codeCoverageIgnoreEnd
        }

        return $this->serializer->unserialize($content, $class);
    }
}
