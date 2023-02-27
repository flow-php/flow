<?php

declare(strict_types=1);

namespace Flow\Serializer;

use Flow\ETL\Exception\RuntimeException;

final class CompressingSerializer implements Serializer
{
    private int $compressionLevel = 9;

    private readonly Serializer $serializer;

    public function __construct()
    {
        $this->serializer = new NativePHPSerializer();
    }

    public function serialize(Serializable $serializable) : string
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

    public function unserialize(string $serialized) : Serializable
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

        return $this->serializer->unserialize($content);
    }
}
