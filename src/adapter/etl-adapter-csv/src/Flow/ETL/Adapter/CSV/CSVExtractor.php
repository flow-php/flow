<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

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
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Generator;

use function count;
use function sprintf;

final class CSVExtractor implements Extractor, FileExtractor, LimitableExtractor, MetadataColumnsExtractor
{
    use Limitable;
    use FileReading;

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
        $hydrator = $context->hydrator();
        $batchSize = $context->config->extractorBatchSize();
        $baseSchema = $this->schema;

        $fileColumns = $this->fileColumns($this->filesystem, $this->path);
        $declared = $fileColumns->declare($baseSchema ?? new Schema());
        $schema = $baseSchema === null ? null : $declared;

        foreach ($this->sourceFiles($this->filesystem, $this->path) as $source) {
            $stream = $this->filesystem->readFrom($source->path);

            $option = csv_detect_separator($stream);

            $separator = $this->separator ?? $option->separator;
            $enclosure = $this->enclosure ?? $option->enclosure;
            $escape = $this->escape ?? $option->escape;
            $constants = $fileColumns->forFile($source, $declared);

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
                        $batch[] = new RawRowValues($constants->fill($rowValues->values));
                    }

                    $rawLines = [];

                    $hydrated = $hydrator->cast($batch, $schema);

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
                }

                $lines->next();
            }

            $batch = [];

            foreach ($encoder->decode($rawLines) as $rowValues) {
                $batch[] = new RawRowValues($constants->fill($rowValues->values));
            }

            $hydrated = $hydrator->cast($batch, $schema);

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

    public function withSchema(Schema $schema): static
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
