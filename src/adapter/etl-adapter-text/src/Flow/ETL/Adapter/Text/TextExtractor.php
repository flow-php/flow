<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\DeclaresPartitionTypes;
use Flow\ETL\Extractor\FileExtractor;
use Flow\ETL\Extractor\Limitable;
use Flow\ETL\Extractor\LimitableExtractor;
use Flow\ETL\Extractor\MetadataColumns;
use Flow\ETL\Extractor\MetadataColumnsExtractor;
use Flow\ETL\Extractor\PartitionColumns;
use Flow\ETL\Extractor\PathFiltering;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\FileListing;
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

    use MetadataColumns;

    use Limitable;
    use DeclaresPartitionTypes;
    use PathFiltering;

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

        $baseSchema = $this->schema();

        $partitionColumns = new PartitionColumns($this->filesystem);
        $partitionNames = $this->partitionNames($partitionColumns, $this->path);

        foreach ((new FileListing($this->filesystem))->list($this->path, $this->filter()) as $listedFile) {
            $stream = $this->filesystem->readFrom($listedFile->path);

            $streamUri = $this->addMetadataColumns ? $stream->path()->uri() : null;
            $partitionValues = [];

            foreach ($stream->path()->partitions() as $partition) {
                $partitionValues[$partition->name] = $partition->value;
            }

            $schema = $baseSchema;

            $schema = $partitionColumns->declare($schema, $partitionNames, $this->declaredPartitionTypes());

            $rawLines = [];

            foreach ($stream->readLines() as $line) {
                $rawLines[] = $line;

                if (count($rawLines) >= $batchSize) {
                    $batch = [];

                    foreach ($encoder->decode($rawLines) as $rowValues) {
                        $row = $rowValues->values;

                        if ($streamUri !== null) {
                            $row['_input_file_uri'] = $streamUri;
                        }

                        $row = $partitionColumns->fill($row, $partitionNames, $partitionValues);

                        $batch[] = new RawRowValues($row);
                    }

                    $rawLines = [];

                    $hydrated = $hydrator->cast($batch, $schema);

                    foreach ($hydrated as $hydratedRow) {
                        $signal = yield new Rows($hydrated->schema(), $hydratedRow);

                        $this->incrementReturnedRows();

                        if ($signal === Signal::STOP || $this->reachedLimit()) {
                            return;
                        }
                    }
                }
            }

            $batch = [];

            foreach ($encoder->decode($rawLines) as $rowValues) {
                $row = $rowValues->values;

                if ($streamUri !== null) {
                    $row['_input_file_uri'] = $streamUri;
                }

                $row = $partitionColumns->fill($row, $partitionNames, $partitionValues);

                $batch[] = new RawRowValues($row);
            }

            $hydrated = $hydrator->cast($batch, $schema);

            foreach ($hydrated as $hydratedRow) {
                $signal = yield new Rows($hydrated->schema(), $hydratedRow);

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
        $schema = $this->schema ?? schema(str_schema('text'));

        $partitionColumns = new PartitionColumns($this->filesystem);

        return $partitionColumns->declare(
            $this->addMetadataColumns ? $schema->add(str_schema('_input_file_uri')) : $schema,
            $this->partitionNames($partitionColumns, $this->path),
        );
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
