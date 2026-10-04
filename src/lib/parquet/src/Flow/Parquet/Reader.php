<?php

declare(strict_types=1);

namespace Flow\Parquet;

use Flow\Filesystem\SourceStream;
use Flow\Filesystem\Stream\NativeLocalSourceStream;
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Engine\AdaptiveParquetEngine;
use Flow\Parquet\Engine\PhpParquetEngine;

use function Flow\Filesystem\DSL\path_real;

final readonly class Reader
{
    private ParquetEngine $engine;

    public function __construct(
        private ByteOrder $byteOrder = ByteOrder::LITTLE_ENDIAN,
        public Options $options = new Options(),
        ?ParquetEngine $engine = null,
    ) {
        $this->engine = $engine ?? new AdaptiveParquetEngine($this->byteOrder, $this->options);
    }

    public static function php(Options $options = new Options()): self
    {
        return new self(options: $options, engine: new PhpParquetEngine(options: $options));
    }

    /**
     * @return ParquetFile<ParquetFileReader>
     */
    public function read(string $path): ParquetFile
    {
        return $this->readStream(NativeLocalSourceStream::open(path_real($path)));
    }

    /**
     * @return ParquetFile<ParquetFileReader>
     */
    public function readStream(SourceStream $stream): ParquetFile
    {
        return new ParquetFile($stream, $this->options, $this->engine->openForRead($stream));
    }
}
