<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

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
use function Flow\ETL\DSL\str_schema;
use function sprintf;

final class CSVExtractor implements Extractor, FileExtractor, LimitableExtractor
{
    use Limitable;
    use PathFiltering;

    /**
     * @var null|int<1, max>
     */
    private ?int $charactersReadInLine = null;

    private bool $emptyToNull = true;

    private ?string $enclosure = null;

    private ?string $escape = null;

    private bool $removeBOM = true;

    private ?Schema $schema = null;

    private ?string $separator = null;

    private bool $withHeader = true;

    private readonly Filesystem $filesystem;

    public function __construct(
        private readonly Path $path,
        Filesystem $filesystem = new NativeLocalFilesystem(),
    ) {
        if (!$filesystem->supports($path)) {
            throw new InvalidArgumentException(sprintf(
                'Filesystem %s serves "%s://" paths, given: "%s". Pass the filesystem that handles '
                . 'this scheme, e.g. from_csv($path, filesystem: aws_s3_filesystem(...)).',
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
        $baseSchema = $this->schema;

        if (
            $baseSchema !== null
            && $shouldPutInputIntoRows
            && $baseSchema->findDefinition('_input_file_uri') === null
        ) {
            $baseSchema = $baseSchema->add(str_schema('_input_file_uri'));
        }

        foreach ((new FileListing($this->filesystem))->list($this->path, $this->filter()) as $listedFile) {
            $stream = $this->filesystem->readFrom($listedFile->path);

            $option = csv_detect_separator($stream);

            $separator = $this->separator ?? $option->separator;
            $enclosure = $this->enclosure ?? $option->enclosure;
            $escape = $this->escape ?? $option->escape;
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

            $lines = (new CSVLineReader($enclosure, $this->charactersReadInLine, $this->removeBOM))->readLines($stream);

            if (!$lines->valid()) {
                $stream->close();

                continue;
            }

            $encoder = new CSVEncoder(
                withHeader: $this->withHeader,
                separator: $separator,
                enclosure: $enclosure,
                escape: $escape,
                emptyToNull: $this->emptyToNull,
            );

            $rawLines = [];

            while (($line = $lines->current()) !== null) {
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

                $lines->next();
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

    public function withBOMRemoval(bool $removeBOM): self
    {
        $this->removeBOM = $removeBOM;

        return $this;
    }

    public function withCharactersReadInLine(int $charactersReadInLine): self
    {
        if ($charactersReadInLine < 1) {
            throw new InvalidArgumentException('Characters read in line must be greater than 0');
        }

        $this->charactersReadInLine = $charactersReadInLine;

        return $this;
    }

    public function withEmptyToNull(bool $emptyToNull): self
    {
        $this->emptyToNull = $emptyToNull;

        return $this;
    }

    public function withEnclosure(string $enclosure): self
    {
        $this->enclosure = $enclosure;

        return $this;
    }

    public function withEscape(string $escape): self
    {
        $this->escape = $escape;

        return $this;
    }

    public function withHeader(bool $withHeader): self
    {
        $this->withHeader = $withHeader;

        return $this;
    }

    public function withSchema(Schema $schema): self
    {
        $this->schema = $schema;

        return $this;
    }

    public function withSeparator(string $separator): self
    {
        $this->separator = $separator;

        return $this;
    }
}
