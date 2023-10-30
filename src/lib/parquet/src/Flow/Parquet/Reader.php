<?php declare(strict_types=1);

namespace Flow\Parquet;

use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\Exception\InvalidArgumentException;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

final class Reader
{
    public function __construct(
        private readonly ByteOrder $byteOrder = ByteOrder::LITTLE_ENDIAN,
        private Options $options = new Options(),
        private readonly LoggerInterface $logger = new NullLogger()
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

        return new ParquetFile($stream, $this->byteOrder, DataConverter::initialize($this->options), $this->options, $this->logger);
    }

    /**
     * @param resource $stream
     */
    public function readStream($stream) : ParquetFile
    {
        /** @psalm-suppress DocblockTypeContradiction */
        if (!\is_resource($stream)) {
            throw new InvalidArgumentException('Given argument is not a valid resource');
        }

        $streamMetadata = \stream_get_meta_data($stream);

        if (!$streamMetadata['seekable']) {
            throw new InvalidArgumentException('Given stream is not seekable');
        }

        return new ParquetFile($stream, $this->byteOrder, DataConverter::initialize($this->options), $this->options, $this->logger);
    }

    public function set(Options $options) : void
    {
        $this->options = $options;
    }
}
