<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel;

use OpenSpout\Common\Entity\Row as OpenSpoutRow;
use OpenSpout\Common\Entity\Style\Style;
use OpenSpout\Writer\AbstractWriterMultiSheets;
use OpenSpout\Writer\Common\Entity\Sheet;
use OpenSpout\Writer\ODS\Options as OdsOptions;
use OpenSpout\Writer\ODS\Writer as OdsWriter;
use OpenSpout\Writer\XLSX\Options as XlsxOptions;
use OpenSpout\Writer\XLSX\Writer as XlsxWriter;

use function array_filter;
use function array_key_exists;
use function str_starts_with;

final class WorkbookManager
{
    private ?string $currentFilePath = null;

    /**
     * @var array<string, bool>
     */
    private array $headersWritten = [];

    /**
     * @var array<string, Sheet>
     */
    private array $sheets = [];

    /**
     * @var array<string, AbstractWriterMultiSheets>
     */
    private array $writers = [];

    public function __construct(
        private readonly ExcelWriter $writerType = ExcelWriter::XLSX,
        private readonly OdsOptions|XlsxOptions|null $options = null,
    ) {}

    public function close(): void
    {
        foreach ($this->writers as $writer) {
            $writer->close();
        }

        $this->writers = [];
        $this->sheets = [];
        $this->headersWritten = [];
        $this->currentFilePath = null;
    }

    public function isHeaderWritten(string $sheetName): bool
    {
        return $this->headersWritten[$this->sheetKey($sheetName)] ?? false;
    }

    public function open(string $filePath): void
    {
        if (array_key_exists($filePath, $this->writers)) {
            $this->currentFilePath = $filePath;

            return;
        }

        $writer = match ($this->writerType) {
            ExcelWriter::XLSX => $this->createXlsxWriter(),
            ExcelWriter::ODS => $this->createOdsWriter(),
        };

        $writer->openToFile($filePath);

        $this->writers[$filePath] = $writer;
        $this->currentFilePath = $filePath;
    }

    /**
     * @param array<string> $headers
     */
    public function writeHeader(string $sheetName, array $headers, ?Style $style = null): void
    {
        $writer = $this->writers[$this->currentFilePath];

        if ($sheetName !== $writer->getCurrentSheet()->getName()) {
            $sheet = $this->getOrCreateSheet($sheetName);
            $writer->setCurrentSheet($sheet);
        }

        $writer->addRow(
            $style !== null ? OpenSpoutRow::fromValuesWithStyle($headers, $style) : OpenSpoutRow::fromValues($headers),
        );

        $key = $this->sheetKey($sheetName);
        $this->headersWritten[$key] = true;
    }

    /**
     * @param array<int, null|bool|float|int|string> $values
     * @param null|array<int, null|Style> $styles
     */
    public function writeRow(string $sheetName, array $values, ?array $styles = null): void
    {
        $writer = $this->writers[$this->currentFilePath];

        if ($sheetName !== $writer->getCurrentSheet()->getName()) {
            $sheet = $this->getOrCreateSheet($sheetName);
            $writer->setCurrentSheet($sheet);
        }

        $writer->addRow(OpenSpoutRow::fromValuesWithStyles(
            $values,
            $styles ? array_filter($styles, static fn(?Style $style): bool => $style !== null) : [],
        ));
    }

    private function countSheetsForCurrentFile(): int
    {
        $prefix = $this->currentFilePath . ':';
        $count = 0;

        foreach ($this->sheets as $key => $_sheet) {
            if (str_starts_with($key, $prefix)) {
                $count++;
            }
        }

        return $count;
    }

    private function createOdsWriter(): OdsWriter
    {
        $options = $this->options instanceof OdsOptions ? $this->options : new OdsOptions();

        return new OdsWriter($options);
    }

    private function createXlsxWriter(): XlsxWriter
    {
        $options = $this->options instanceof XlsxOptions ? $this->options : new XlsxOptions();

        return new XlsxWriter($options);
    }

    private function getOrCreateSheet(string $sheetName): Sheet
    {
        $key = $this->sheetKey($sheetName);

        if (array_key_exists($key, $this->sheets)) {
            return $this->sheets[$key];
        }

        $writer = $this->writers[$this->currentFilePath];
        $currentSheet = $writer->getCurrentSheet();

        if ($currentSheet->getName() === 'Sheet1' && $this->countSheetsForCurrentFile() === 0) {
            $currentSheet->setName($sheetName);
            $this->sheets[$key] = $currentSheet;

            return $currentSheet;
        }

        foreach ($writer->getSheets() as $existingSheet) {
            if ($existingSheet->getName() === $sheetName) {
                $this->sheets[$key] = $existingSheet;

                return $existingSheet;
            }
        }

        $newSheet = $writer->addNewSheetAndMakeItCurrent();
        $newSheet->setName($sheetName);
        $this->sheets[$key] = $newSheet;

        return $newSheet;
    }

    private function sheetKey(string $sheetName): string
    {
        return $this->currentFilePath . ':' . $sheetName;
    }
}
