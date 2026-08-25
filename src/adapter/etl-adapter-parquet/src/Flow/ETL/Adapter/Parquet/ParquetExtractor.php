<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\FileExtractor;
use Flow\ETL\Extractor\Limitable;
use Flow\ETL\Extractor\LimitableExtractor;
use Flow\ETL\Extractor\PathFiltering;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
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

final class ParquetExtractor implements Extractor, FileExtractor, LimitableExtractor
{
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
        $shouldPutInputIntoRows = $context->config->shouldPutInputIntoRows();
        $hydrator = $context->hydrator();
        $batchSize = $context->config->extractorBatchSize();

        $fileOffset = $this->offset ?? 0;

        foreach ($this->readers($context) as $fileData) {
            $fileRows = $fileData['file']->metadata()->rowsNumber();

            if ($fileOffset > $fileRows) {
                $fileData['stream']->close();
                $fileOffset -= $fileRows;

                continue;
            }

            $flowSchema = $this->schemaConverter->toFlow($fileData['file']->schema());
            $streamUri = $shouldPutInputIntoRows ? $fileData['stream']->path()->uri() : null;

            if (count($this->columns)) {
                $flowSchema = $flowSchema->keep(...$this->columns);
            }

            if ($streamUri !== null && $flowSchema->findDefinition('_input_file_uri') === null) {
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
                    foreach ($hydrator->hydrate($encoder->decode($rawBatch), $flowSchema) as $hydratedRow) {
                        $this->incrementReturnedRows();
                        $signal = yield new Rows($hydratedRow);

                        if ($signal === Signal::STOP || $this->reachedLimit()) {
                            return;
                        }
                    }

                    $rawBatch = [];
                }
            }

            foreach ($hydrator->hydrate($encoder->decode($rawBatch), $flowSchema) as $hydratedRow) {
                $this->incrementReturnedRows();
                $signal = yield new Rows($hydratedRow);

                if ($signal === Signal::STOP || $this->reachedLimit()) {
                    return;
                }
            }

            $fileOffset = max($fileOffset - $fileRows, 0);
            $fileData['stream']->close();
        }
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
    private function readers(FlowContext $context): Generator
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
}
