<?php

declare(strict_types=1);

namespace Flow\Serializer;

use Flow\ETL\Exception\RuntimeException;

final class CompressingSerializer implements Serializer
{
    public function __construct(
        private readonly Serializer $serializer,
        private readonly int $compressionLevel = 9
    ) {
    }

    public function serialize(Serializable $serializable) : string
    {
        if (!\function_exists('gzcompress')) {
            // @codeCoverageIgnoreStart
            throw new RuntimeException("'ext-zlib' is missing in, compression impossible due to lack of gzcompress.");
            // @codeCoverageIgnoreEnd
        }

        /**
         * @phpstan-ignore-next-line
         */
        return \base64_encode(\gzcompress($this->serializer->serialize($serializable), $this->compressionLevel));
    }

    public function unserialize(string $serialized) : Serializable
    {
        if (!\function_exists('gzcompress')) {
            // @codeCoverageIgnoreStart
            throw new RuntimeException("'ext-zlib' is missing in, decompression impossible due to lack of gzuncompress.");
            // @codeCoverageIgnoreEnd
        }

        /**
         * @phpstan-ignore-next-line
         */
        return $this->serializer->unserialize(\gzuncompress(\base64_decode($serialized, true)));
    }
}
