<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel;

use Flow\ETL\Adapter\Excel\RowsNormalizer\ExcelRowsNormalizer;
use Flow\ETL\Adapter\Excel\Sheet\SheetNameAssertion;
use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Loader\FileLoader;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\Filesystem\Path;
use OpenSpout\Common\Entity\Style\Style;
use OpenSpout\Writer\ODS\Options as OdsOptions;
use OpenSpout\Writer\XLSX\Options as XlsxOptions;
use Throwable;

use function is_string;

final class ExcelLoader implements Closure, FileLoader, Loader
{
    private ?CellStyler $cellStyler = null;

    private string $dateFormat = 'Y-m-d';

    private string $dateTimeFormat = 'Y-m-d H:i:s';

    private ?Style $headerStyle = null;

    private readonly Path $path;

    private ?string $sheetName = null;

    private ?string $sheetNameEntryName = null;

    private string $timeFormat = 'H:i:s';

    private bool $withHeader = true;

    private ?WorkbookManager $workbookManager = null;

    private OdsOptions|XlsxOptions|null $writerOptions = null;

    private ?ExcelWriter $writerType = null;

    public function __construct(Path $path)
    {
        if (!$path->isLocal()) {
            throw new InvalidArgumentException(
                'Only local filesystem paths are supported by ExcelLoader due to OpenSpout limitations.',
            );
        }

        $this->path = $path;
    }

    public function closure(FlowContext $context): void
    {
        if ($this->workbookManager !== null) {
            $this->workbookManager->close();
            $this->workbookManager = null;
        }

        $context->streams()->closeStreams($this->path);
    }

    public function destination(): Path
    {
        return $this->path;
    }

    public function load(Rows $rows, FlowContext $context): void
    {
        if (!$rows->count()) {
            return;
        }

        $context->telemetry()->loadingStarted($this, [
            TelemetryAttributes::ATTR_LOADER_DESTINATION_URI => $this->path->uri(),
        ]);

        try {
            $normalizer = new ExcelRowsNormalizer(
                dateFormat: $this->dateFormat,
                dateTimeFormat: $this->dateTimeFormat,
                timeFormat: $this->timeFormat,
            );

            $stream = $context->streams()->writeTo($this->path, $rows->partitions()->toArray());

            $manager = $this->getWorkbookManager();
            $manager->open($stream->path()->path());

            foreach ($rows as $rowIndex => $row) {
                $sheetName = $this->resolveSheetName($row);

                $rowForExcel = $this->sheetNameEntryName !== null && $row->has($this->sheetNameEntryName)
                    ? $row->remove($this->sheetNameEntryName)
                    : $row;

                if ($this->withHeader && !$manager->isHeaderWritten($sheetName)) {
                    $headers = $normalizer->headers($rowForExcel);
                    $manager->writeHeader($sheetName, $headers, $this->headerStyle);
                }

                $values = $normalizer->normalize($rowForExcel);
                $styles = $this->resolveCellStyles($rowForExcel, $rowIndex, $sheetName);
                $manager->writeRow($sheetName, $values, $styles);
            }

            $context->telemetry()->loadingCompleted($this, [TelemetryAttributes::ATTR_LOADING_ROWS => $rows->count()]);
        } catch (Throwable $e) {
            $context->telemetry()->loadingFailed($this, $e);

            throw $e;
        }
    }

    public function withCellStyler(CellStyler $styler): self
    {
        $this->cellStyler = $styler;

        return $this;
    }

    public function withDateFormat(string $format): self
    {
        $this->dateFormat = $format;

        return $this;
    }

    public function withDateTimeFormat(string $format): self
    {
        $this->dateTimeFormat = $format;

        return $this;
    }

    public function withHeader(bool $withHeader = true): self
    {
        $this->withHeader = $withHeader;

        return $this;
    }

    public function withHeaderStyle(Style $style): self
    {
        $this->headerStyle = $style;

        return $this;
    }

    public function withSheetName(?string $sheetName): self
    {
        if ($sheetName !== null && $this->sheetNameEntryName !== null) {
            throw new InvalidArgumentException(
                'Cannot set both sheetName and sheetNameFromEntry. These options are mutually exclusive.',
            );
        }

        if ($sheetName !== null) {
            SheetNameAssertion::assert($sheetName);
        }

        $this->sheetName = $sheetName;

        return $this;
    }

    public function withSheetNameFromEntry(string $entryName): self
    {
        if ($this->sheetName !== null) {
            throw new InvalidArgumentException(
                'Cannot set both sheetName and sheetNameFromEntry. These options are mutually exclusive.',
            );
        }

        $this->sheetNameEntryName = $entryName;

        return $this;
    }

    public function withTimeFormat(string $format): self
    {
        $this->timeFormat = $format;

        return $this;
    }

    public function withWriter(ExcelWriter $writer): self
    {
        $this->writerType = $writer;

        return $this;
    }

    public function withWriterOptions(OdsOptions|XlsxOptions $options): self
    {
        $this->writerOptions = $options;

        return $this;
    }

    private function getWorkbookManager(): WorkbookManager
    {
        if ($this->workbookManager === null) {
            $this->workbookManager = new WorkbookManager(
                writerType: $this->resolveWriterType(),
                options: $this->writerOptions,
            );
        }

        return $this->workbookManager;
    }

    /**
     * @return null|array<int, null|Style>
     */
    private function resolveCellStyles(Row $row, int $rowIndex, string $sheetName): ?array
    {
        if ($this->cellStyler === null) {
            return null;
        }

        $styles = [];
        $columnIndex = 0;

        foreach ($row->entries() as $entry) {
            $styles[$columnIndex] = $this->cellStyler->style($entry, $rowIndex + 1, $columnIndex, $sheetName);
            $columnIndex++;
        }

        return $styles;
    }

    private function resolveSheetName(Row $row): string
    {
        if ($this->sheetNameEntryName !== null && $row->has($this->sheetNameEntryName)) {
            // @mago-expect analysis:mixed-assignment
            $value = $row->get($this->sheetNameEntryName)->value();

            if (is_string($value) && $value !== '') {
                SheetNameAssertion::assert($value);

                return $value;
            }
        }

        return $this->sheetName ?? 'Sheet1';
    }

    private function resolveWriterType(): ExcelWriter
    {
        if ($this->writerType !== null) {
            return $this->writerType;
        }

        return match ((string) $this->path->extension()) {
            'ods' => ExcelWriter::ODS,
            default => ExcelWriter::XLSX,
        };
    }
}
