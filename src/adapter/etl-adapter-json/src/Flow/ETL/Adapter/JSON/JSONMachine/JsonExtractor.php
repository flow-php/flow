<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\JSONMachine;

use function Flow\ETL\DSL\{array_to_rows};
use Flow\ETL\Extractor\{FileExtractor, Limitable, LimitableExtractor, PathFiltering, Signal};
use Flow\ETL\Row\Schema;
use Flow\ETL\{Extractor, FlowContext};
use Flow\Filesystem\{Path, SourceStream};
use JsonMachine\Items;
use JsonMachine\JsonDecoder\ExtJsonDecoder;

final class JsonExtractor implements Extractor, FileExtractor, LimitableExtractor
{
    use Limitable;
    use PathFiltering;

    private bool $jsonl = false;

    private ?string $pointer = null;

    private bool $pointerToEntryName = false;

    private ?Schema $schema = null;

    public function __construct(
        private readonly Path $path,
    ) {
        $this->resetLimit();
    }

    public function asJsonl() : self
    {
        $this->jsonl = true;

        return $this;
    }

    public function extract(FlowContext $context) : \Generator
    {
        $shouldPutInputIntoRows = $context->config->shouldPutInputIntoRows();

        $iterator = $this->getIterator();

        foreach ($context->streams()->list($this->path, $this->filter()) as $stream) {
            /**
             * @var array|object $rowData
             */
            foreach ($iterator($stream) as $rowData) {

                $row = (array) $rowData;

                if ($shouldPutInputIntoRows) {
                    $row['_input_file_uri'] = $stream->path()->uri();
                }

                if ($this->pointer !== null && $this->pointerToEntryName) {
                    $row = [$this->pointer => $row];
                }

                if (!\count($row)) {
                    continue;
                }

                $signal = yield array_to_rows([$row], $context->entryFactory(), $stream->path()->partitions(), $this->schema);
                $this->incrementReturnedRows();

                if ($signal === Signal::STOP || $this->reachedLimit()) {
                    $context->streams()->closeStreams($this->path);

                    return;
                }
            }

            $stream->close();
        }
    }

    public function source() : Path
    {
        return $this->path;
    }

    /**
     * @param string $pointer
     * @param bool $pointerToEntryName - when true pointer will be used as entry name for extracted data
     */
    public function withPointer(string $pointer, bool $pointerToEntryName = false) : self
    {
        $this->pointer = $pointer;
        $this->pointerToEntryName = $pointerToEntryName;

        return $this;
    }

    public function withSchema(Schema $schema) : self
    {
        $this->schema = $schema;

        return $this;
    }

    private function getIterator() : callable
    {
        if (!$this->jsonl) {
            return fn (SourceStream $stream) : \Generator => (new Items($stream->iterate(8 * 1024), $this->readerOptions()))->getIterator();
        }

        // JSONL iterator modes
        if ($this->pointer !== null) {
            return function (SourceStream $stream) : \Generator {
                foreach ($stream->readLines() as $jsonLine) {
                    return Items::fromString($jsonLine, $this->readerOptions())->getIterator();
                }
            };
        }

        return function (SourceStream $stream) : \Generator {
            foreach ($stream->readLines() as $jsonLine) {
                $jsonData = Items::fromString($jsonLine, $this->readerOptions());
                $row = \iterator_to_array($jsonData);
                yield $row;
            }
        };
    }

    /**
     * @return array{pointer?: string, decoder: ExtJsonDecoder}
     */
    private function readerOptions() : array
    {
        $options = [
            'decoder' => new ExtJsonDecoder(true),
        ];

        if ($this->pointer !== null) {
            $options['pointer'] = $this->pointer;
        }

        return $options;
    }
}
