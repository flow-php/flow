<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\JSONMachine;

use Flow\ETL\Adapter\JSON\JSONEncoder;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\FileExtractor;
use Flow\ETL\Extractor\Limitable;
use Flow\ETL\Extractor\LimitableExtractor;
use Flow\ETL\Extractor\MetadataColumns;
use Flow\ETL\Extractor\MetadataColumnsExtractor;
use Flow\ETL\Extractor\PathFiltering;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\FileListing;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Generator;
use JsonMachine\Items;
use JsonMachine\JsonDecoder\ExtJsonDecoder;

use function count;
use function Flow\ETL\DSL\str_schema;
use function sprintf;

final class JsonExtractor implements Extractor, FileExtractor, LimitableExtractor, MetadataColumnsExtractor
{
    use MetadataColumns;

    use Limitable;
    use PathFiltering;

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
                . 'this scheme, e.g. from_json($path, filesystem: aws_s3_filesystem(...)).',
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
        $baseSchema = $this->schema === null ? null : $this->schema();

        foreach ((new FileListing($this->filesystem))->list($this->path, $this->filter()) as $listedFile) {
            $stream = $this->filesystem->readFrom($listedFile->path);

            $streamUri = $this->addMetadataColumns ? $stream->path()->uri() : null;
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

            /**
             * @var array<string, mixed> $rowData
             */
            foreach ((new Items($stream->iterate(8 * 1024), $this->readerOptions()))->getIterator() as $rowData) {
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
                            return;
                        }
                    }

                    $rawBatch = [];
                }
            }

            foreach ($hydrator->cast($encoder->decode($rawBatch), $schema) as $hydratedRow) {
                $signal = yield Rows::partitioned([$hydratedRow], $partitions);

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
        if ($this->schema === null) {
            throw SchemaNotDerivableException::extractor(self::class);
        }

        if ($this->addMetadataColumns) {
            return $this->schema->add(str_schema('_input_file_uri'));
        }

        return $this->schema;
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
