<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel;

use function Flow\ETL\DSL\array_to_rows;
use Flow\ETL\{Adapter\Excel\Sheet\SheetNameAssertion,
    Adapter\Excel\Sheet\SheetsManager,
    Exception\InvalidArgumentException,
    Extractor,
    FlowContext,
    Loader\Closure};
use Flow\ETL\Extractor\{FileExtractor, Limitable, LimitableExtractor, PathFiltering, Signal};
use Flow\Filesystem\{Path, SourceStream};
use OpenSpout\Common\Entity\{Cell, Row};
use OpenSpout\Reader\ODS\Reader as OdsReader;
use OpenSpout\Reader\XLSX\Reader as XlsxReader;

final class ExcelExtractor implements Closure, Extractor, FileExtractor, LimitableExtractor
{
    use Limitable;
    use PathFiltering;

    private bool $convertEmptyToNull = true;

    private ?int $offset = null;

    private XlsxReader|OdsReader|null $reader = null;

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

    public function closure(FlowContext $context) : void
    {
        $this->reader()->close();
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
                $signal = yield array_to_rows($row, $context->entryFactory());
                $this->incrementReturnedRows();

                if ($signal === Signal::STOP || $this->reachedLimit()) {
                    return;
                }
            }
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

    public function withSheetName(string $sheetName) : self
    {
        SheetNameAssertion::assert($sheetName);

        $this->sheetName = $sheetName;

        return $this;
    }

    private function createRowsFromCells(Row $row) : array
    {
        return \array_map(
            fn (Cell $cell) =>
                // Convert empty values to nullables if allowed
                $this->convertEmptyToNull && '' === $cell->getValue() ? null : $cell->getValue(),
            $row->getCells()
        );
    }

    private function extendRowData(int $headersCount, array $rowData) : array
    {
        $rowDataCount = \count($rowData);

        if ($headersCount > $rowDataCount) {
            \array_push(
                $rowData,
                ...\array_map(
                    static fn (int $i) => null,
                    \range(1, $headersCount - $rowDataCount)
                )
            );
        }

        return $rowData;
    }

    private function extractRows(SourceStream $stream, array $headers, int $offset) : array
    {
        try {
            $this->reader()->open($stream->path()->path());

            $manager = new SheetsManager($this->reader()->getSheetIterator());

            $rows = [];
            $previousRowDataCount = 0;

            $sheet = $this->sheetName ? $manager->get($this->sheetName) : $manager->first();

            foreach ($sheet->getRowIterator() as $rowIndex => $row) {
                if (1 === $rowIndex && $this->withHeader) {
                    $headers = $this->createRowsFromCells($row);

                    continue;
                }

                // Skip till offset is reach
                if ($offset > $rowIndex) {
                    continue;
                }

                // ODS format reader skips empty cells when reading rows
                $rowData = $this->extendRowData($previousRowDataCount, $this->createRowsFromCells($row));
                $previousRowDataCount = \count($rowData);

                if ($this->withHeader) {
                    $rowData = \array_combine($headers, $rowData);
                }

                $rows[] = $rowData;
            }

            return $rows;
        } catch (\Throwable $e) {
            throw new InvalidArgumentException('Failed to open file: ' . $e->getMessage(), previous: $e);
        }
    }

    private function reader() : XlsxReader|OdsReader
    {
        if (null === $this->reader) {
            $this->reader = match ($this->path->extension()) {
                'xlsx' => new XlsxReader(),
                'ods' => new OdsReader(),
                default => throw new InvalidArgumentException('Unsupported file extension: ' . ($this->path->extension() ?: 'n/a')),
            };
        }

        return $this->reader;
    }
}
