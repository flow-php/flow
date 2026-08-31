<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel;

use Flow\ETL\Adapter\Excel\Sheet\SheetNameAssertion;
use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Filesystem\FilesSink;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Loader\Discardable;
use Flow\ETL\Loader\FileLoader;
use Flow\ETL\Loader\Partitioning;
use Flow\ETL\Loader\PartitioningLoader;
use Flow\ETL\Loader\PartitionRouter;
use Flow\ETL\Row;
use Flow\ETL\Row\TypedRowValues;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use OpenSpout\Common\Entity\Style\Style;
use OpenSpout\Writer\ODS\Options as OdsOptions;
use OpenSpout\Writer\XLSX\Options as XlsxOptions;
use Throwable;

use function array_keys;
use function is_string;
use function sprintf;

final class ExcelLoader implements Closure, Discardable, FileLoader, Loader, PartitioningLoader
{
    private PartitionRouter $router;

    private readonly Filesystem $filesystem;

    private SaveMode $saveMode = SaveMode::ExceptionIfExists;

    private ?FilesSink $files = null;

    private ?WorkbookManager $workbook = null;

    private ?CellStyler $cellStyler = null;

    private string $dateFormat = 'Y-m-d';

    private string $dateTimeFormat = 'Y-m-d H:i:s';

    private ?ExcelEncoder $encoder = null;

    private ?Style $headerStyle = null;

    private readonly Path $path;

    private ?string $sheetName = null;

    private ?string $sheetNameEntryName = null;

    private string $timeFormat = '%H:%I:%S';

    private bool $withHeader = true;

    private OdsOptions|XlsxOptions|null $writerOptions = null;

    private ?ExcelWriter $writerType = null;

    public function __construct(Path $path, Filesystem $filesystem = new NativeLocalFilesystem())
    {
        if (!$filesystem->supports($path)) {
            throw new InvalidArgumentException(sprintf(
                'Filesystem %s serves "%s://" paths, given: "%s". Pass the filesystem that handles '
                . 'this scheme, e.g. to_excel($path, filesystem: aws_s3_filesystem(...)).',
                $filesystem::class,
                $filesystem->mount()->protocol,
                $path->uri(),
            ));
        }

        $this->filesystem = $filesystem;
        $this->router = new PartitionRouter(Partitioning::none());

        if (!$path->isLocal()) {
            throw new InvalidArgumentException(
                'Only local filesystem paths are supported by ExcelLoader due to OpenSpout limitations.',
            );
        }

        $this->path = $path;
    }

    public function partitionBy(Partitioning $partitioning): static
    {
        $this->router = new PartitionRouter($partitioning);

        return $this;
    }

    public function closure(FlowContext $context): void
    {
        $this->workbook?->close();
        $this->workbook = null;

        $this->files?->publish();
        $this->files = null;
    }

    public function discard(FlowContext $context): void
    {
        $this->workbook?->close();
        $this->workbook = null;

        $this->files?->abandon();
        $this->files = null;
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
            $encoder = $this->encoder();

            foreach ($this->router->route($rows) as [$partitions, $group]) {
                $dehydrated = $context->hydrator()->dehydrate($group);

                $stream = ($this->files ??= new FilesSink($this->filesystem, $this->path, $this->saveMode))->writeTo(
                    $partitions->toArray(),
                );

                $manager =
                    $this->workbook ??= new WorkbookManager(
                        writerType: $this->resolveWriterType(),
                        options: $this->writerOptions,
                    );
                $manager->open($stream->path()->path());

                foreach ($group as $rowIndex => $row) {
                    $sheetName = $this->resolveSheetName($row);

                    $rowSchema = $this->sheetNameEntryName !== null && $row->has($this->sheetNameEntryName)
                        ? $group->schema()->gracefulRemove($this->sheetNameEntryName)
                        : $group->schema();

                    $typed = $dehydrated[$rowIndex];
                    $values = $typed->values;
                    $types = $typed->types;
                    $metadata = $typed->metadata;

                    if ($this->sheetNameEntryName !== null) {
                        unset(
                            $values[$this->sheetNameEntryName],
                            $types[$this->sheetNameEntryName],
                            $metadata[$this->sheetNameEntryName],
                        );
                    }

                    if ($this->withHeader && !$manager->isHeaderWritten($sheetName)) {
                        $manager->writeHeader(
                            $sheetName,
                            $encoder->encodeHeader(array_keys($values)),
                            $this->headerStyle,
                        );
                    }

                    $styles = $this->resolveCellStyles($row, $rowSchema, $rowIndex, $sheetName);
                    /** @var array<int, null|bool|float|int|string> $cells */
                    $cells = $encoder->encode([new TypedRowValues($values, $types, $metadata)])[0];
                    $manager->writeRow($sheetName, $cells, $styles);
                }
            }

            $context->telemetry()->loadingCompleted($this, [TelemetryAttributes::ATTR_LOADING_ROWS => $rows->count()]);
        } catch (Throwable $e) {
            $context->telemetry()->loadingFailed($this, $e);

            throw $e;
        }
    }

    public function saveMode(SaveMode $mode): static
    {
        $this->saveMode = $mode;

        return $this;
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

    private function encoder(): ExcelEncoder
    {
        return $this->encoder ??= new ExcelEncoder(
            dateTimeFormat: $this->dateTimeFormat,
            dateFormat: $this->dateFormat,
            timeFormat: $this->timeFormat,
        );
    }

    /**
     * @return null|array<int, null|Style>
     */
    private function resolveCellStyles(Row $row, Schema $schema, int $rowIndex, string $sheetName): ?array
    {
        if ($this->cellStyler === null) {
            return null;
        }

        $styles = [];
        $columnIndex = 0;

        foreach ($schema->definitions() as $definition) {
            $styles[$columnIndex] = $this->cellStyler->style(
                $row->get($definition->entry()->name()),
                $definition,
                $rowIndex + 1,
                $columnIndex,
                $sheetName,
            );
            $columnIndex++;
        }

        return $styles;
    }

    private function resolveSheetName(Row $row): string
    {
        if ($this->sheetNameEntryName !== null && $row->has($this->sheetNameEntryName)) {
            $value = $row->get($this->sheetNameEntryName);

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
