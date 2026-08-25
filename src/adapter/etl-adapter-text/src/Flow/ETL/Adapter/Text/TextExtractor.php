<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\FileExtractor;
use Flow\ETL\Extractor\Limitable;
use Flow\ETL\Extractor\LimitableExtractor;
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

final class TextExtractor implements Extractor, FileExtractor, LimitableExtractor
{
    use Limitable;
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
        $shouldPutInputIntoRows = $context->config->shouldPutInputIntoRows();
        $hydrator = $context->hydrator();
        $batchSize = $context->config->extractorBatchSize();
        $encoder = new TextEncoder();

        $baseSchema = $this->schema($shouldPutInputIntoRows);

        foreach ((new FileListing($this->filesystem))->list($this->path, $this->filter()) as $listedFile) {
            $stream = $this->filesystem->readFrom($listedFile->path);

            $streamUri = $shouldPutInputIntoRows ? $stream->path()->uri() : null;
            $partitions = $stream->path()->partitions();

            $schema = $baseSchema;

            foreach ($partitions as $partition) {
                if ($schema->findDefinition($partition->name) === null) {
                    $schema = $schema->add(str_schema($partition->name));
                }
            }

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

                        foreach ($partitions as $partition) {
                            $row[$partition->name] = $partition->value;
                        }

                        $batch[] = new RawRowValues($row);
                    }

                    $rawLines = [];

                    foreach ($hydrator->cast($batch, $schema) as $hydratedRow) {
                        $signal = yield Rows::partitioned([$hydratedRow], $partitions);

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

                foreach ($partitions as $partition) {
                    $row[$partition->name] = $partition->value;
                }

                $batch[] = new RawRowValues($row);
            }

            foreach ($hydrator->cast($batch, $schema) as $hydratedRow) {
                $signal = yield Rows::partitioned([$hydratedRow], $partitions);

                $this->incrementReturnedRows();

                if ($signal === Signal::STOP || $this->reachedLimit()) {
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

    private function schema(bool $shouldPutInputIntoRows): Schema
    {
        if ($shouldPutInputIntoRows) {
            return schema(str_schema('text'), str_schema('_input_file_uri'));
        }

        return schema(str_schema('text'));
    }
}
