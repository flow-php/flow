<?php

declare(strict_types=1);

namespace Flow\Serializer;

use Flow\ETL\Exception\RuntimeException;

final class CompressingSerializer implements Serializer
{
    private int $compressionLevel;

    /**
     * @var Serializer
     */
    private Serializer $serializer;

    public function __construct(Serializer $serializer, int $compressionLevel = 9)
    {
        $this->serializer = $serializer;
        $this->compressionLevel = $compressionLevel;
    }

    public function serialize(Serializable $serializable) : string
    {
        if (!\function_exists('gzcompress')) {
            throw new RuntimeException("'ext-zlib' is missing in, compression impossible due to lack of gzcompress.");
        }

        /**
         * @phpstan-ignore-next-line
         */
        return \base64_encode(\gzcompress($this->serializer->serialize($serializable), $this->compressionLevel));
    }

    public function unserialize(string $serialized) : Serializable
    {
        if (!\function_exists('gzcompress')) {
            throw new RuntimeException("'ext-zlib' is missing in, decompression impossible due to lack of gzuncompress.");
        }

        /**
         * @phpstan-ignore-next-line
         */
        return $this->serializer->unserialize(\gzuncompress(\base64_decode($serialized, true)));
    }
}
