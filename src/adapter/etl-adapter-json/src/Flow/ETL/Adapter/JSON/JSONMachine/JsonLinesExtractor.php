<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\JSONMachine;

use Flow\ETL\Adapter\JSON\JSONEncoder;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\FileExtractor;
use Flow\ETL\Extractor\FileReading;
use Flow\ETL\Extractor\Limitable;
use Flow\ETL\Extractor\LimitableExtractor;
use Flow\ETL\Extractor\MetadataColumnsExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Generator;
use JsonMachine\Items;
use JsonMachine\JsonDecoder\ExtJsonDecoder;

use function count;
use function iterator_to_array;
use function sprintf;

final class JsonLinesExtractor implements Extractor, FileExtractor, LimitableExtractor, MetadataColumnsExtractor
{
    use Limitable;
    use FileReading;

    private ?string $pointer = null;

    private bool $pointerToEntryName = false;

    private ?Schema $schema = null;

    private readonly Filesystem $filesystem;

    public function __construct(
        private readonly Path $path,
        Filesystem $filesystem = new NativeLocalFilesystem(),
    ) {
        if (!$filesystem->supports($path)) {
            throw new InvalidArgumentException(sprintf(
                'Filesystem %s serves "%s://" paths, given: "%s". Pass the filesystem that handles '
                . 'this scheme, e.g. from_json_lines($path, filesystem: aws_s3_filesystem(...)).',
                $filesystem::class,
                $filesystem->mount()->protocol,
                $path->uri(),
            ));
        }

        $this->filesystem = $filesystem;
        $this->resetLimit();
    }

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        $hydrator = $context->hydrator();
        $batchSize = $context->config->extractorBatchSize();
        $encoder = new JSONEncoder();
        $baseSchema = $this->schema;

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

        $fileColumns = $this->fileColumns($this->filesystem, $this->path);
        $declared = $fileColumns->declare($baseSchema ?? new Schema());
        $schema = $baseSchema === null ? null : $declared;

        foreach ($this->sourceFiles($this->filesystem, $this->path) as $source) {
            $stream = $this->filesystem->readFrom($source->path);

            $constants = $fileColumns->forFile($source, $declared);

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

                    $row = $constants->fill($row);

                    $rawBatch[] = $row;

                    if (count($rawBatch) >= $batchSize) {
                        $hydrated = $hydrator->cast($encoder->decode($rawBatch), $schema);

                        if ($baseSchema === null) {
                            $hydrated = $fileColumns->apply($hydrated);
                        }

                        foreach ($hydrated as $hydratedRow) {
                            $signal = yield Rows::trusted($hydrated->schema(), [$hydratedRow]);

                            $this->incrementReturnedRows();

                            if ($signal === Signal::STOP || $this->reachedLimit()) {
                                return;
                            }
                        }

                        $rawBatch = [];
                    }
                }
            }

            $hydrated = $hydrator->cast($encoder->decode($rawBatch), $schema);

            if ($baseSchema === null) {
                $hydrated = $fileColumns->apply($hydrated);
            }

            foreach ($hydrated as $hydratedRow) {
                $signal = yield Rows::trusted($hydrated->schema(), [$hydratedRow]);

                $this->incrementReturnedRows();

                if ($signal === Signal::STOP || $this->reachedLimit()) {
                    return;
                }
            }

            $stream->close();
        }
    }

    public function schema(): Schema
    {
        $schema = $this->schema;

        if ($schema === null) {
            throw SchemaNotDerivableException::extractor(self::class);
        }

        return $this->fileColumns($this->filesystem, $this->path)->declare($schema);
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

    public function withSchema(Schema $schema): static
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
