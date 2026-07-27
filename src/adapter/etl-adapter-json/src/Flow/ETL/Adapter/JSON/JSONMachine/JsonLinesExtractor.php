<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\JSONMachine;

use Flow\ETL\Adapter\JSON\JSONEncoder;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\FileExtractor;
use Flow\ETL\Extractor\Limitable;
use Flow\ETL\Extractor\LimitableExtractor;
use Flow\ETL\Extractor\PathFiltering;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\Path;
use Generator;
use JsonMachine\Items;
use JsonMachine\JsonDecoder\ExtJsonDecoder;

use function count;
use function Flow\ETL\DSL\str_schema;
use function iterator_to_array;

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

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        $shouldPutInputIntoRows = $context->config->shouldPutInputIntoRows();
        $hydrator = $context->hydrator();
        $batchSize = $context->config->extractorBatchSize();
        $encoder = new JSONEncoder();
        $baseSchema = $this->schema;

        if (
            $baseSchema !== null
            && $shouldPutInputIntoRows
            && $baseSchema->findDefinition('_input_file_uri') === null
        ) {
            $baseSchema = $baseSchema->add(str_schema('_input_file_uri'));
        }

        // JSONL iterator modes
        $lineIterator = match ($this->pointer) {
            null => function (string $jsonLine): Generator {
                $jsonData = Items::fromString($jsonLine, $this->readerOptions());
                $row = iterator_to_array($jsonData);
                yield $row;
            },
            default => fn(string $jsonLine): Generator => (
                /** Pointed Iterator */
                Items::fromString($jsonLine, $this->readerOptions())->getIterator()
            ),
        };

        foreach ($context->streams()->list($this->path, $this->filter()) as $stream) {
            $streamUri = $shouldPutInputIntoRows ? $stream->path()->uri() : null;
            $partitions = $stream->path()->partitions();

            $schema = $baseSchema;

            if ($schema !== null) {
                foreach ($partitions as $partition) {
                    if ($schema->findDefinition($partition->name) === null) {
                        $schema = $schema->add(str_schema($partition->name));
                    }
                }
            }

            $rawBatch = [];

            foreach ($stream->readLines() as $jsonLine) {
                /**
                 * @var array<string, mixed> $rowData
                 */
                foreach ($lineIterator($jsonLine) as $rowData) {
                    $row = $rowData;

                    if ($this->pointer !== null && $this->pointerToEntryName) {
                        $row = [$this->pointer => $row];
                    }

                    if (!count($row)) {
                        continue;
                    }

                    if ($streamUri !== null) {
                        $row['_input_file_uri'] = $streamUri;
                    }

                    foreach ($partitions as $partition) {
                        $row[$partition->name] = $partition->value;
                    }

                    $rawBatch[] = $row;

                    if (count($rawBatch) >= $batchSize) {
                        foreach ($hydrator->cast($encoder->decode($rawBatch), $schema) as $hydratedRow) {
                            $signal = yield Rows::partitioned([$hydratedRow], $partitions);

                            $this->incrementReturnedRows();

                            if ($signal === Signal::STOP || $this->reachedLimit()) {
                                $context->streams()->closeStreams($this->path);

                                return;
                            }
                        }

                        $rawBatch = [];
                    }
                }
            }

            foreach ($hydrator->cast($encoder->decode($rawBatch), $schema) as $hydratedRow) {
                $signal = yield Rows::partitioned([$hydratedRow], $partitions);

                $this->incrementReturnedRows();

                if ($signal === Signal::STOP || $this->reachedLimit()) {
                    $context->streams()->closeStreams($this->path);

                    return;
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
