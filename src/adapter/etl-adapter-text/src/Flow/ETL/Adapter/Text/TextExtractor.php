<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\FileExtractor;
use Flow\ETL\Extractor\FileReading;
use Flow\ETL\Extractor\Limitable;
use Flow\ETL\Extractor\LimitableExtractor;
use Flow\ETL\Extractor\MetadataColumnsExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Generator;

use function count;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function sprintf;

final class TextExtractor implements Extractor, FileExtractor, LimitableExtractor, MetadataColumnsExtractor
{
    private ?Schema $schema = null;

    use Limitable;
    use FileReading;

    private readonly Filesystem $filesystem;

    public function __construct(
        private readonly Path $path,
        Filesystem $filesystem = new NativeLocalFilesystem(),
    ) {
        if (!$filesystem->supports($path)) {
            throw new InvalidArgumentException(sprintf(
                'Filesystem %s serves "%s://" paths, given: "%s". Pass the filesystem that handles '
                . 'this scheme, e.g. from_text($path, filesystem: aws_s3_filesystem(...)).',
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
        $encoder = new TextEncoder();

        $baseSchema = $this->schema ?? schema(str_schema('text'));

        $fileColumns = $this->fileColumns($this->filesystem, $this->path);
        $schema = $fileColumns->declare($baseSchema);

        foreach ($this->sourceFiles($this->filesystem, $this->path) as $source) {
            $stream = $this->filesystem->readFrom($source->path);

            $constants = $fileColumns->forFile($source, $schema);

            $rawLines = [];

            foreach ($stream->readLines() as $line) {
                $rawLines[] = $line;

                if (count($rawLines) >= $batchSize) {
                    $batch = [];

                    foreach ($encoder->decode($rawLines) as $rowValues) {
                        $batch[] = new RawRowValues($constants->fill($rowValues->values));
                    }

                    $rawLines = [];

                    $hydrated = $hydrator->cast($batch, $schema);

                    foreach ($hydrated as $hydratedRow) {
                        $signal = yield Rows::trusted($hydrated->schema(), [$hydratedRow]);

                        $this->incrementReturnedRows();

                        if ($signal === Signal::STOP || $this->reachedLimit()) {
                            return;
                        }
                    }
                }
            }

            $batch = [];

            foreach ($encoder->decode($rawLines) as $rowValues) {
                $batch[] = new RawRowValues($constants->fill($rowValues->values));
            }

            $hydrated = $hydrator->cast($batch, $schema);

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
        return $this->fileColumns($this->filesystem, $this->path)->declare($this->schema ?? schema(str_schema('text')));
    }

    public function source(): Path
    {
        return $this->path;
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
