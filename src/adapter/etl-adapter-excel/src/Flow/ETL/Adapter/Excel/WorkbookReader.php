<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel;

use Flow\ETL\Adapter\Excel\Sheet\OpenSheet;
use Flow\ETL\Adapter\Excel\Sheet\SheetCells;
use Flow\ETL\Adapter\Excel\Sheet\SheetsManager;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\SourceFile;
use Flow\Filesystem\Path;
use OpenSpout\Reader\ODS\Reader as OdsReader;
use OpenSpout\Reader\XLSX\Reader as XlsxReader;
use Throwable;

final readonly class WorkbookReader
{
    public function __construct(
        private ExcelReadOptions $options,
        private ExcelFormatDetector $detector,
    ) {}

    public function open(Path $path): OpenSheet
    {
        $reader = match ($this->options->format ?? $this->detector->detect($path)) {
            ExcelReader::XLSX => new XlsxReader(),
            ExcelReader::ODS => new OdsReader(),
        };

        try {
            $reader->open($path->path());
            $sheets = new SheetsManager($reader->getSheetIterator());
            $sheet = $this->options->sheetName === null ? $sheets->first() : $sheets->get($this->options->sheetName);
        } catch (Throwable $e) {
            $reader->close();

            throw new InvalidArgumentException('Failed to open file: ' . $e->getMessage(), previous: $e);
        }

        return new OpenSheet(
            $reader,
            (new SheetCells($sheet, $this->options->withHeader, $this->options->offset))->rows(),
            new ExcelEncoder(
                withHeader: $this->options->withHeader,
                convertEmptyToNull: $this->options->convertEmptyToNull,
            ),
        );
    }

    public function sheet(SourceFile $source): WorkbookSheet
    {
        return new WorkbookSheet($source->path, $this);
    }
}
