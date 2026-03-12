<?php

declare(strict_types=1);

namespace Flow\Parquet;

use function Flow\Filesystem\DSL\path_real;
use Flow\Filesystem\{SourceStream, Stream\NativeLocalSourceStream};
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\ParquetFile\Data\DataConverter;

final readonly class Reader
{
    public function __construct(
        private ByteOrder $byteOrder = ByteOrder::LITTLE_ENDIAN,
        public Options $options = new Options(),
    ) {
    }

    public function read(string $path) : ParquetFile
    {
        return new ParquetFile(
            NativeLocalSourceStream::open(path_real($path)),
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
