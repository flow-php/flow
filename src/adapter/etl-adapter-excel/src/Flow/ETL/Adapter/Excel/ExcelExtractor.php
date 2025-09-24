<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel;

use function Flow\ETL\DSL\{array_to_rows};
use Flow\ETL\{Adapter\Excel\Sheet\SheetNameAssertion,
    Adapter\Excel\Sheet\SheetsManager,
    Exception\InvalidArgumentException,
    Extractor,
    FlowContext,
    Schema};
use Flow\ETL\Extractor\{FileExtractor, Limitable, LimitableExtractor, PathFiltering, Signal};
use Flow\Filesystem\{Path, SourceStream};
use OpenSpout\Common\Entity\{Cell, Row};
use OpenSpout\Reader\ODS\Reader as OdsReader;
use OpenSpout\Reader\XLSX\Reader as XlsxReader;

final class ExcelExtractor implements Extractor, FileExtractor, LimitableExtractor
{
    use Limitable;
    use PathFiltering;

    private bool $convertEmptyToNull = true;

    private ?int $offset = null;

    private XlsxReader|OdsReader|null $reader = null;

    private ?Schema $schema = null;

    private ?string $sheetName = null;

    private bool $withHeader = true;

    public function __construct(private readonly Path $path)
    {
        if (!$this->path->isLocal()) {
            // We can't use resources (returned by \fopen) since they are not supported by the OpenSpout library.
            // They are not supported because OpenSpout library uses php built in ZipArchive library, which doesn't support resources, only local paths.
            throw new InvalidArgumentException('Only local filesystem paths are supported by ExcelExtractor due to the limitation of underlying library.');
        }

        $this->resetLimit();
    }

    public function extract(FlowContext $context) : \Generator
    {
        $headers = [];

        // Offset must be a positive number
        $offset = $this->offset ?? 1;

        if (!$this->withHeader) {
            $offset++;
        }

        foreach ($context->streams()->list($this->path, $this->filter()) as $stream) {
            foreach ($this->extractRows($stream, $headers, $offset) as $row) {
                // Ensure $row is an array before passing to array_to_rows
                $signal = yield array_to_rows(\is_array($row) ? $row : [], $context->entryFactory(), $stream->path()->partitions(), schema: $this->schema);
                $this->incrementReturnedRows();

                if ($signal === Signal::STOP || $this->reachedLimit()) {
                    $stream->close();

                    return;
                }
            }

            $stream->close();
        }
    }

    public function source() : Path
    {
        return $this->path;
    }

    public function withConvertEmptyToNull(bool $convertEmptyToNull) : self
    {
        $this->convertEmptyToNull = $convertEmptyToNull;

        return $this;
    }

    public function withHeader(bool $withHeader) : self
    {
        $this->withHeader = $withHeader;

        return $this;
    }

    public function withOffset(int $offset) : self
    {
        if ($offset < 1) {
            throw new InvalidArgumentException('Offset must be greater or equal to 1');
        }

        $this->offset = $offset;

        return $this;
    }

    public function withReader(ExcelReader $reader) : self
    {
        $this->reader = match ($reader) {
            ExcelReader::XLSX => new XlsxReader(),
            ExcelReader::ODS => new OdsReader(),
        };

        return $this;
    }

    public function withSchema(Schema $schema) : self
    {
        $this->schema = $schema;

        return $this;
    }

    public function withSheetName(string $sheetName) : self
    {
        SheetNameAssertion::assert($sheetName);

        $this->sheetName = $sheetName;

        return $this;
    }

    /**
     * @return array<int, mixed>
     */
    private function createRowsFromCells(Row $row, int $previousRowDataCount = 0) : array
    {
        $rowData = \array_map(
            // Convert empty values to nullables if allowed
            fn (Cell $cell) => $this->convertEmptyToNull && '' === $cell->getValue() ? null : $cell->getValue(),
            $row->getCells()
        );

        // Expand columns to the size of the previous row
        for ($i = \count($rowData); $i < $previousRowDataCount; $i++) {
            $rowData[$i] = null;
        }

        return $rowData;
    }

    /**
     * @param array<int, string> $headers
     */
    private function extractRows(SourceStream $stream, array $headers, int $offset) : \Generator
    {
        $reader = $this->reader($stream);

        try {
            $reader->open($stream->path()->path());

            $manager = new SheetsManager($reader->getSheetIterator());

            $previousRowDataCount = 0;

            $sheet = $this->sheetName ? $manager->get($this->sheetName) : $manager->first();

            foreach ($sheet->getRowIterator() as $rowIndex => $sheetRow) {
                if (1 === $rowIndex && $this->withHeader) {
                    $headersRaw = $this->createRowsFromCells($sheetRow);
                    // Convert headers to strings for array_combine compatibility
                    $headers = \array_map(
                        fn ($header) => \is_scalar($header) ? (string) $header : '',
                        $headersRaw
                    );

                    continue;
                }

                // Skip till offset is reach
                if ($offset > $rowIndex) {
                    continue;
                }

                // ODS format reader skips empty cells when reading rows
                $row = $this->createRowsFromCells($sheetRow, $previousRowDataCount);
                $previousRowDataCount = \count($row);

                if ($this->withHeader) {
                    yield \array_combine($headers, $row);
                } else {
                    yield $row;
                }
            }

            $reader->close();
        } catch (\Throwable $e) {
            throw new InvalidArgumentException('Failed to open file: ' . $e->getMessage(), previous: $e);
        }
    }

    private function reader(SourceStream $stream) : XlsxReader|OdsReader
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
                if (\str_starts_with($line, "\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1")) {
                    return $this->reader = new XlsxReader();
                }

                // ZIP signature: 50 4B 03 04
                if (\str_starts_with($line, "\x50\x4B\x03\x04")) {
                    $zip = new \ZipArchive();

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
                throw new InvalidArgumentException('Unsupported file format: ' . ($stream->path()->extension() ?: 'n/a'));
            }
        }

        return $this->reader;
    }
}
