<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Exception\InvalidArgumentException;
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
use Flow\Filesystem\SourceStream;
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile;
use Flow\Parquet\Reader;
use Generator;

use function count;
use function Flow\ETL\DSL\str_schema;
use function max;
use function sprintf;

final class ParquetExtractor implements Extractor, FileExtractor, LimitableExtractor, MetadataColumnsExtractor
{
    private ?Schema $schema = null;

    use MetadataColumns;

    use Limitable;
    use PathFiltering;

    private ByteOrder $byteOrder = ByteOrder::LITTLE_ENDIAN;

    /**
     * @var array<string>
     */
    private array $columns = [];

    private ?ParquetEngine $engine = null;

    private ?int $offset = null;

    private Options $options;

    private SchemaConverter $schemaConverter;

    private readonly Filesystem $filesystem;

    /**
     * @param Path $path
     */
    public function __construct(
        private readonly Path $path,
        Filesystem $filesystem = new NativeLocalFilesystem(),
    ) {
        if (!$filesystem->supports($path)) {
            throw new InvalidArgumentException(sprintf(
                'Filesystem %s serves "%s://" paths, given: "%s". Pass the filesystem that handles '
                . 'this scheme, e.g. from_parquet($path, filesystem: aws_s3_filesystem(...)).',
                $filesystem::class,
                $filesystem->mount()->protocol,
                $path->uri(),
            ));
        }

        $this->filesystem = $filesystem;
        $this->resetLimit();
        $this->schemaConverter = new SchemaConverter();
        $this->options = Options::default();
    }

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        $hydrator = $context->hydrator();
        $batchSize = $context->config->extractorBatchSize();

        $fileOffset = $this->offset ?? 0;
        $promisedSchema = $this->schema === null ? null : $this->schema();

        foreach ($this->readers() as $fileData) {
            $fileRows = $fileData['file']->metadata()->rowsNumber();

            if ($fileOffset > $fileRows) {
                $fileData['stream']->close();
                $fileOffset -= $fileRows;

                continue;
            }

            $flowSchema = $this->schemaConverter->toFlow($fileData['file']->schema());
            $streamUri = $this->addMetadataColumns ? $fileData['stream']->path()->uri() : null;

            if (count($this->columns)) {
                $flowSchema = $flowSchema->keep(...$this->columns);
            }

            if ($streamUri !== null) {
                $flowSchema = $flowSchema->add(str_schema('_input_file_uri'));
            }

            $encoder = new ParquetEncoder($fileData['file']->schema());

            $rawBatch = [];

            foreach ($fileData['file']->values($this->columns, $this->limit(), $fileOffset) as $row) {
                if ($streamUri !== null) {
                    $row['_input_file_uri'] = $streamUri;
                }

                $rawBatch[] = $row;

                if (count($rawBatch) >= $batchSize) {
                    $hydrated = $hydrator->hydrate($encoder->decode($rawBatch), $promisedSchema ?? $flowSchema);

                    foreach ($hydrated as $hydratedRow) {
                        $this->incrementReturnedRows();
                        $signal = yield new Rows($hydrated->schema(), $hydratedRow);

                        if ($signal === Signal::STOP || $this->reachedLimit()) {
                            return;
                        }
                    }

                    $rawBatch = [];
                }
            }

            $hydrated = $hydrator->hydrate($encoder->decode($rawBatch), $promisedSchema ?? $flowSchema);

            foreach ($hydrated as $hydratedRow) {
                $this->incrementReturnedRows();
                $signal = yield new Rows($hydrated->schema(), $hydratedRow);

                if ($signal === Signal::STOP || $this->reachedLimit()) {
                    return;
                }
            }

            $fileOffset = max($fileOffset - $fileRows, 0);
            $fileData['stream']->close();
        }
    }

    public function schema(): Schema
    {
        $schema = $this->schema;

        if ($schema === null) {
            $schema = new Schema();

            foreach ($this->readers() as $fileData) {
                $fileSchema = $this->schemaConverter->toFlow($fileData['file']->schema());

                if (count($this->columns)) {
                    $fileSchema = $fileSchema->keep(...$this->columns);
                }

                $schema = $schema->merge($fileSchema);
                $fileData['stream']->close();
            }
        }

        if ($this->addMetadataColumns) {
            $schema = $schema->add(str_schema('_input_file_uri'));
        }

        return $schema;
    }

    public function source(): Path
    {
        return $this->path;
    }

    public function withByteOrder(ByteOrder $byteOrder): self
    {
        $this->byteOrder = $byteOrder;

        return $this;
    }

    /**
     * @param array<string> $columns
     */
    public function withColumns(array $columns): self
    {
        $this->columns = $columns;

        return $this;
    }

    public function withEngine(?ParquetEngine $engine): self
    {
        $this->engine = $engine;

        return $this;
    }

    public function withOffset(int $offset): self
    {
        if ($offset < 0) {
            throw new InvalidArgumentException('Offset must be greater or equal to 0');
        }

        $this->offset = $offset;

        return $this;
    }

    public function withOptions(Options $options): self
    {
        $this->options = $options;

        return $this;
    }

    /**
     * @return \Generator<int, array{file: ParquetFile, stream: SourceStream}>
     */
    private function readers(): Generator
    {
        foreach ((new FileListing($this->filesystem))->list($this->path, $this->filter()) as $listedFile) {
            $stream = $this->filesystem->readFrom($listedFile->path);

            yield [
                'file' => (new Reader(
                    byteOrder: $this->byteOrder,
                    options: $this->options,
                    engine: $this->engine,
                ))->readStream($stream),
                'stream' => $stream,
            ];
        }
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
