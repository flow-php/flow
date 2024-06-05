<?php

declare(strict_types=1);

namespace Flow\Parquet;

use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Stream\LocalStream;

final class Reader
{
    public function __construct(
        private readonly ByteOrder $byteOrder = ByteOrder::LITTLE_ENDIAN,
        private Options $options = new Options()
    ) {
    }

    public function read(string $path) : ParquetFile
    {
        if (!\file_exists($path)) {
            throw new InvalidArgumentException("File {$path} does not exist");
        }

        $stream = \fopen($path, 'rb');

        if (!\is_resource($stream)) {
            throw new InvalidArgumentException("File {$path} is not a valid resource");
        }

        $streamMetadata = \stream_get_meta_data($stream);

        if (!$streamMetadata['seekable']) {
            throw new InvalidArgumentException("File {$path} is not seekable");
        }

        return new ParquetFile(new LocalStream($stream), $this->byteOrder, DataConverter::initialize($this->options), $this->options);
    }

    public function readStream(Stream $stream) : ParquetFile
    {
        return new ParquetFile($stream, $this->byteOrder, DataConverter::initialize($this->options), $this->options);
    }

    public function set(Options $options) : void
    {
        $this->options = $options;
    }
}
