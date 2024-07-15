<?php

declare(strict_types=1);

namespace Flow\Parquet;

use Flow\Filesystem\{Path, SourceStream, Stream\NativeLocalSourceStream};
use Flow\Parquet\Data\DataConverter;

final class Reader
{
    public function __construct(
        private readonly ByteOrder $byteOrder = ByteOrder::LITTLE_ENDIAN,
        public readonly Options $options = new Options()
    ) {
    }

    public function read(string $path) : ParquetFile
    {
        return new ParquetFile(
            NativeLocalSourceStream::open(Path::realpath($path)),
            $this->byteOrder,
            DataConverter::initialize($this->options),
            $this->options
        );
    }

    public function readStream(SourceStream $stream) : ParquetFile
    {
        return new ParquetFile($stream, $this->byteOrder, DataConverter::initialize($this->options), $this->options);
    }
}
