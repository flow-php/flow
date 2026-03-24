<?php

declare(strict_types=1);

namespace Flow\Parquet;

use function Flow\Filesystem\DSL\path_real;
use Flow\Filesystem\{SourceStream, Stream\NativeLocalSourceStream};
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Engine\{AdaptiveParquetEngine, ArrowParquetEngine, PhpParquetEngine};

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

    public static function arrow(Options $options = new Options()) : self
    {
        return new self(options: $options, engine: new ArrowParquetEngine($options));
    }

    public static function php(Options $options = new Options()) : self
    {
        return new self(options: $options, engine: new PhpParquetEngine(options: $options));
    }

    public function read(string $path) : ParquetFile
    {
        return new ParquetFile(
            NativeLocalSourceStream::open(path_real($path)),
            $this->byteOrder,
            $this->options,
            $this->engine,
        );
    }

    public function readStream(SourceStream $stream) : ParquetFile
    {
        return new ParquetFile(
            $stream,
            $this->byteOrder,
            $this->options,
            $this->engine,
        );
    }
}
