<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\JSONMachine;

use Flow\ETL\Extractor;
use Flow\ETL\Extractor\FileExtractor;
use Flow\ETL\Extractor\Limitable;
use Flow\ETL\Extractor\LimitableExtractor;
use Flow\ETL\Extractor\PathFiltering;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Schema;
use Flow\Filesystem\Path;
use JsonMachine\Items;
use JsonMachine\JsonDecoder\ExtJsonDecoder;

use function Flow\ETL\DSL\array_to_rows;

final class JsonLinesExtractor implements Extractor, FileExtractor, LimitableExtractor
{
    use Limitable;
    use PathFiltering;

    private ?string $pointer = null;

    private bool $pointerToEntryName = false;

    private ?Schema $schema = null;

    public function __construct(
        private readonly Path $path,
    ) {
        $this->resetLimit();
    }

    public function extract(FlowContext $context): \Generator
    {
        $shouldPutInputIntoRows = $context->config->shouldPutInputIntoRows();

        // JSONL iterator modes
        $lineIterator = match ($this->pointer) {
            null => function (string $jsonLine): \Generator {
                $jsonData = Items::fromString($jsonLine, $this->readerOptions());
                $row = \iterator_to_array($jsonData);
                yield $row;
            },
            default => fn(string $jsonLine): \Generator => (
                /** Pointed Iterator */
                Items::fromString($jsonLine, $this->readerOptions())->getIterator()
            ),
        };

        foreach ($context->streams()->list($this->path, $this->filter()) as $stream) {
            $uri = $stream->path()->uri();

            foreach ($stream->readLines() as $jsonLine) {
                /**
                 * @var array<string, mixed>|object $rowData
                 */
                foreach ($lineIterator($jsonLine) as $rowData) {
                    $row = (array) $rowData;

                    if ($shouldPutInputIntoRows) {
                        $row['_input_file_uri'] = $uri;
                    }

                    if ($this->pointer !== null && $this->pointerToEntryName) {
                        $row = [$this->pointer => $row];
                    }

                    if (!\count($row)) {
                        continue;
                    }

                    $signal = yield array_to_rows(
                        [$row],
                        $context->entryFactory(),
                        $stream->path()->partitions(),
                        $this->schema,
                    );

                    $this->incrementReturnedRows();

                    if ($signal === Signal::STOP || $this->reachedLimit()) {
                        $context->streams()->closeStreams($this->path);

                        return;
                    }
                }
            }

            $stream->close();
        }
    }

    public function source(): Path
    {
        return $this->path;
    }

    /**
     * @param string $pointer
     * @param bool $pointerToEntryName - when true pointer will be used as entry name for extracted data
     */
    public function withPointer(string $pointer, bool $pointerToEntryName = false): self
    {
        $this->pointer = $pointer;
        $this->pointerToEntryName = $pointerToEntryName;

        return $this;
    }

    public function withSchema(Schema $schema): self
    {
        $this->schema = $schema;

        return $this;
    }

    /**
     * @return array{pointer?: string, decoder: ExtJsonDecoder}
     */
    private function readerOptions(): array
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
