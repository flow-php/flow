<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel;

use Flow\ETL\Adapter\Excel\Sheet\SheetNameAssertion;
use Flow\ETL\Adapter\Excel\Sheet\SheetsManager;
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
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\FileListing;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Flow\Filesystem\SourceStream;
use Generator;
use OpenSpout\Common\Entity\Cell;
use OpenSpout\Common\Entity\Row;
use OpenSpout\Reader\ODS\Reader as OdsReader;
use OpenSpout\Reader\XLSX\Reader as XlsxReader;
use Throwable;
use ZipArchive;

use function array_map;
use function count;
use function Flow\ETL\DSL\str_schema;
use function sprintf;
use function str_starts_with;

final class ExcelExtractor implements Extractor, FileExtractor, LimitableExtractor, MetadataColumnsExtractor
{
    use MetadataColumns;

    use Limitable;
    use PathFiltering;

    private bool $convertEmptyToNull = true;

    private ?int $offset = null;

    private XlsxReader|OdsReader|null $reader = null;

    private ?Schema $schema = null;

    private ?string $sheetName = null;

    private bool $withHeader = true;

    private readonly Filesystem $filesystem;

    public function __construct(
        private readonly Path $path,
        Filesystem $filesystem = new NativeLocalFilesystem(),
    ) {
        if (!$filesystem->supports($path)) {
            throw new InvalidArgumentException(sprintf(
                'Filesystem %s serves "%s://" paths, given: "%s". Pass the filesystem that handles '
                . 'this scheme, e.g. from_excel($path, filesystem: aws_s3_filesystem(...)).',
                $filesystem::class,
                $filesystem->mount()->protocol,
                $path->uri(),
            ));
        }

        $this->filesystem = $filesystem;

        if (!$this->path->isLocal()) {
            // We can't use resources (returned by \fopen) since they are not supported by the OpenSpout library.
            // They are not supported because OpenSpout library uses php built in ZipArchive library, which doesn't support resources, only local paths.
            throw new InvalidArgumentException(
                'Only local filesystem paths are supported by ExcelExtractor due to the limitation of underlying library.',
            );
        }

        $this->resetLimit();
    }

    /**
     * @return Generator<int, \Flow\ETL\Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        // Offset must be a positive number
        $offset = $this->offset ?? 1;
        $hydrator = $context->hydrator();
        $batchSize = $context->config->extractorBatchSize();

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

            $encoder = new ExcelEncoder(withHeader: $this->withHeader, convertEmptyToNull: $this->convertEmptyToNull);
            $rawCells = [];

            foreach ($this->extractRows($stream, $offset) as $cells) {
                $rawCells[] = $cells;

                if (count($rawCells) >= $batchSize) {
                    $batch = [];

                    foreach ($encoder->decode($rawCells) as $rowValues) {
                        $row = $rowValues->values;

                        if ($streamUri !== null) {
                            $row['_input_file_uri'] = $streamUri;
                        }

                        foreach ($partitions as $partition) {
                            $row[$partition->name] = $partition->value;
                        }

                        $batch[] = new RawRowValues($row);
                    }

                    $rawCells = [];

                    foreach ($hydrator->cast($batch, $schema) as $hydratedRow) {
                        $signal = yield Rows::partitioned([$hydratedRow], $partitions);

                        $this->incrementReturnedRows();

                        if ($signal === Signal::STOP || $this->reachedLimit()) {
                            $stream->close();

                            return;
                        }
                    }
                }
            }

            $batch = [];

            foreach ($encoder->decode($rawCells) as $rowValues) {
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
                    $stream->close();

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

    public function withConvertEmptyToNull(bool $convertEmptyToNull): self
    {
        $this->convertEmptyToNull = $convertEmptyToNull;

        return $this;
    }

    public function withHeader(bool $withHeader): self
    {
        $this->withHeader = $withHeader;

        return $this;
    }

    public function withOffset(int $offset): self
    {
        if ($offset < 1) {
            throw new InvalidArgumentException('Offset must be greater or equal to 1');
        }

        $this->offset = $offset;

        return $this;
    }

    public function withReader(ExcelReader $reader): self
    {
        $this->reader = match ($reader) {
            ExcelReader::XLSX => new XlsxReader(),
            ExcelReader::ODS => new OdsReader(),
        };

        return $this;
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }

    public function withSheetName(string $sheetName): self
    {
        SheetNameAssertion::assert($sheetName);

        $this->sheetName = $sheetName;

        return $this;
    }

    /**
     * @return array<int, mixed>
     */
    private function createRowsFromCells(Row $row, int $previousRowDataCount = 0): array
    {
        $rowData = array_map(static fn(Cell $cell) => $cell->getValue(), $row->cells);

        // Expand columns to the size of the previous row
        for ($i = count($rowData); $i < $previousRowDataCount; $i++) {
            $rowData[$i] = null;
        }

        return $rowData;
    }

    /**
     * @return Generator<int, array<int, mixed>>
     */
    private function extractRows(SourceStream $stream, int $offset): Generator
    {
        $reader = $this->reader($stream);

        try {
            $reader->open($stream->path()->path());

            $manager = new SheetsManager($reader->getSheetIterator());

            $previousRowDataCount = 0;

            $sheet = $this->sheetName ? $manager->get($this->sheetName) : $manager->first();

            $rowIndex = 0;

            foreach ($sheet->getRowIterator() as $sheetRow) {
                $rowIndex++;

                if (1 === $rowIndex && $this->withHeader) {
                    yield $this->createRowsFromCells($sheetRow);

                    continue;
                }

                // Skip till offset is reach
                if ($offset > $rowIndex) {
                    continue;
                }

                // ODS format reader skips empty cells when reading rows
                $row = $this->createRowsFromCells($sheetRow, $previousRowDataCount);
                $previousRowDataCount = count($row);

                yield $row;
            }

            $reader->close();
        } catch (Throwable $e) {
            throw new InvalidArgumentException('Failed to open file: ' . $e->getMessage(), previous: $e);
        }
    }

    private function reader(SourceStream $stream): XlsxReader|OdsReader
    {
        if (null === $this->reader) {
            $this->reader = match ($stream->path()->extension()) {
                'xlsx' => new XlsxReader(),
                'ods' => new OdsReader(),
                default => null,
            };

            if (null === $this->reader) {
                $line = $stream->read(8, 0);

                // XLS signature: D0 CF 11 E0 A1 B1 1A E1
                if (str_starts_with($line, "\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1")) {
                    return $this->reader = new XlsxReader();
                }

                // ZIP signature: 50 4B 03 04
                if (str_starts_with($line, "\x50\x4B\x03\x04")) {
                    $zip = new ZipArchive();

                    if ($zip->open($stream->path()->path())) {
                        $mimetype = $zip->getFromName('mimetype');
                        $zip->close();

                        $this->reader = match ($mimetype) {
                            'application/vnd.oasis.opendocument.spreadsheet' => new OdsReader(),
                            // Other zip-based file formats
                            default => new XlsxReader(),
                        };
                    }
                }
            }

            if (!$this->reader) {
                throw new InvalidArgumentException(
                    'Unsupported file format: ' . ($stream->path()->extension() ?: 'n/a'),
                );
            }
        }

        return $this->reader;
    }
}
