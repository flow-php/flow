<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\IO\Loader;

use DateTimeInterface;
use Flow\ETL\Adapter\Excel\Functions\SheetNameAssertion;
use Flow\ETL\Adapter\Excel\IO\Extractor\WorkbookReader;
use Flow\ETL\Foundation\Describe\ConfiguredFilesystem;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Filesystem\SaveMode;
use Flow\ETL\IO\Loader;
use Flow\ETL\IO\Loader\Closure;
use Flow\ETL\IO\Loader\Discardable;
use Flow\ETL\IO\Loader\File\FileCommit;
use Flow\ETL\IO\Loader\File\FileLoader;
use Flow\ETL\IO\Loader\File\Part;
use Flow\ETL\IO\Loader\File\PartitionRouter;
use Flow\ETL\IO\Loader\Partitioning;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use OpenSpout\Common\Entity\Style\Style;
use OpenSpout\Writer\ODS\Options as OdsOptions;
use OpenSpout\Writer\XLSX\Options as XlsxOptions;
use SensitiveParameter;

use function array_filter;
use function array_values;
use function constant;
use function extension_loaded;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function is_string;

final class ExcelLoader implements Closure, Discardable, FileLoader, Loader
{
    /**
     * OpenSpout measures sheet names and cut cells with mb_* functions: native mbstring, or symfony's polyfill over
     * iconv. Its composer.json requires neither, so a worker gets whichever the parent has.
     */
    private const array MULTIBYTE_EXTENSIONS = ['iconv', 'mbstring'];

    private PartitionRouter $router;

    private Partitioning $partitioning;

    private readonly Filesystem $filesystem;

    private SaveMode $saveMode = SaveMode::ExceptionIfExists;

    private ?FileCommit $files = null;

    private bool $admitted = false;

    private ?Part $task = null;

    private ?WorkbookManager $workbook = null;

    private ?CellStyler $cellStyler = null;

    private string $dateFormat = 'yyyy-mm-dd';

    private string $dateTimeFormat = 'yyyy-mm-dd hh:mm:ss';

    /**
     * OpenSpout registers every Style instance as its own cell format, so a Style built per cell grows styles.xml
     * by one entry per cell - which every reader of the workbook then has to parse.
     *
     * @var array<string, Style>
     */
    private array $formatStyles = [];

    private ?ExcelEncoder $encoder = null;

    private ?Style $headerStyle = null;

    private readonly Path $path;

    private ?string $sheetName = null;

    private ?string $sheetNameColumn = null;

    private string $timeFormat = '%H:%I:%S';

    private bool $withHeader = true;

    private OdsOptions|XlsxOptions|null $writerOptions = null;

    private ?ExcelWriter $writerType = null;

    public function __construct(Path $path, Filesystem $filesystem = new NativeLocalFilesystem())
    {
        $filesystem->supports($path) || throw InvalidArgumentException::unsupportedPath($filesystem, $path, 'to_excel');

        $this->filesystem = $filesystem;
        $this->partitioning = Partitioning::none();
        $this->router = new PartitionRouter($this->partitioning);

        if (!$path->isLocal()) {
            throw new InvalidArgumentException(
                'Only local filesystem paths are supported by ExcelLoader due to OpenSpout limitations.',
            );
        }

        $this->path = $path;
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        $loader = (new self(
            Path::fromArray(type_array()->assert($data['path'] ?? null)),
            $rebuild->of($data['filesystem'] ?? null, ConfiguredFilesystem::class)->filesystem(),
        ))
            ->saveMode(type_enum(SaveMode::class)->assert(constant(
                SaveMode::class . '::' . type_string()->assert($data['save_mode'] ?? null),
            )))
            ->partitionBy(Partitioning::fromArray(type_array()->assert($data['partitioning'] ?? null)))
            ->withDateFormat(type_string()->assert($data['date_format'] ?? null))
            ->withDateTimeFormat(type_string()->assert($data['date_time_format'] ?? null))
            ->withHeader(type_boolean()->assert($data['header'] ?? null))
            ->withTimeFormat(type_string()->assert($data['time_format'] ?? null))
            ->withSheetName(type_optional(type_string())->assert($data['sheet_name'] ?? null));

        if (($data['sheet_name_column'] ?? null) !== null) {
            $loader->withSheetNameFromColumn(type_string()->assert($data['sheet_name_column']));
        }

        if (($data['writer'] ?? null) !== null) {
            $loader->withWriter(type_enum(ExcelWriter::class)->assert(constant(
                ExcelWriter::class . '::' . type_string()->assert($data['writer']),
            )));
        }

        if (($data['task'] ?? null) !== null) {
            $loader->task = Part::fromArray(type_array()->assert($data['task']));
        }

        return $loader;
    }

    /**
     * A cell styler, a header style and writer options are OpenSpout or user objects that do not describe: a loader
     * holding one cannot be shipped to a worker.
     */
    public function describe(): ?Description
    {
        if ($this->cellStyler !== null || $this->headerStyle !== null || $this->writerOptions !== null) {
            return null;
        }

        return Description::compose(
            self::class,
            [
                'path' => $this->path->normalize(),
                'filesystem' => new ConfiguredFilesystem($this->filesystem),
                'save_mode' => $this->saveMode->name,
                'partitioning' => $this->partitioning->normalize(),
                'task' => $this->task?->normalize(),
                'date_format' => $this->dateFormat,
                'date_time_format' => $this->dateTimeFormat,
                'header' => $this->withHeader,
                'sheet_name' => $this->sheetName,
                'sheet_name_column' => $this->sheetNameColumn,
                'time_format' => $this->timeFormat,
                'writer' => $this->writerType?->name,
            ],
            extensions: [
                ...WorkbookReader::EXTENSIONS,
                ...array_values(array_filter(self::MULTIBYTE_EXTENSIONS, extension_loaded(...))),
            ],
        );
    }

    public function partitionBy(Partitioning $partitioning): static
    {
        $this->partitioning = $partitioning;
        $this->router = new PartitionRouter($partitioning);

        return $this;
    }

    public function closure(): void
    {
        $this->workbook?->close();
        $this->workbook = null;

        // the commit stays reachable until it succeeds: discard() after a failed commit still aborts it
        if ($this->task === null) {
            $this->files()->commit([0]);
        }

        $this->files = null;
    }

    public function discard(): void
    {
        $this->workbook?->close();
        $this->workbook = null;

        $this->files?->abort();
        $this->files = null;
    }

    public function commit(string $run, int $tasks): ?FileCommit
    {
        return FileCommit::admitted(
            $this->filesystem,
            $this->path,
            $this->saveMode,
            $this->partitioning->by->count() > 0,
            $run,
            $tasks,
        );
    }

    public function destination(): Path
    {
        return $this->path;
    }

    public function partitioning(): Partitioning
    {
        return $this->partitioning;
    }

    public function inTask(Part $task): static
    {
        $loader = (new self($this->path, $this->filesystem))
            ->saveMode($this->saveMode)
            ->partitionBy($this->partitioning)
            ->withDateFormat($this->dateFormat)
            ->withDateTimeFormat($this->dateTimeFormat)
            ->withHeader($this->withHeader)
            ->withTimeFormat($this->timeFormat)
            ->withSheetName($this->sheetName);

        if ($this->sheetNameColumn !== null) {
            $loader->withSheetNameFromColumn($this->sheetNameColumn);
        }

        if ($this->cellStyler !== null) {
            $loader->withCellStyler($this->cellStyler);
        }

        if ($this->headerStyle !== null) {
            $loader->withHeaderStyle($this->headerStyle);
        }

        if ($this->writerType !== null) {
            $loader->withWriter($this->writerType);
        }

        if ($this->writerOptions !== null) {
            $loader->withWriterOptions($this->writerOptions);
        }

        $loader->task = $task;

        return $loader;
    }

    public function load(Rows $rows, Backend $backend): void
    {
        if (!$rows->count()) {
            return;
        }

        $files = $this->files();

        // an Ignore over an existing workbook writes nothing: OpenSpout opens the path itself, not a stream
        if (!$this->admitted) {
            return;
        }

        $encoder = $this->encoder();

        foreach ($this->router->route($rows) as [$partitions, $group]) {
            $stream = $files->open($partitions->toArray());

            $manager =
                $this->workbook ??= new WorkbookManager(
                    writerType: $this->resolveWriterType(),
                    options: $this->writerOptions,
                );
            $manager->open($stream->path()->path());

            $sheetColumn =
                $this->sheetNameColumn !== null && $group->schema()->findDefinition($this->sheetNameColumn) !== null
                    ? $this->sheetNameColumn
                    : null;
            $written = $sheetColumn === null
                ? $group
                : $group->project($group->schema()->gracefulRemove($sheetColumn), $backend);
            $sheetValues = $sheetColumn === null ? null : $group->column($sheetColumn)->values();

            /** @var array<array-key, list<int>> $sheets */
            $sheets = [];

            for ($i = 0; $i < $group->count(); $i++) {
                $sheets[$this->resolveSheetName($sheetValues === null ? null : $sheetValues[$i])][] = $i;
            }

            foreach ($sheets as $sheetName => $indices) {
                $sheetName = (string) $sheetName;
                $sheetRows = $written->gather($indices);

                if ($this->withHeader && !$manager->isHeaderWritten($sheetName)) {
                    $manager->writeHeader(
                        $sheetName,
                        $encoder->encodeHeader(array_values($sheetRows->schema()->names())),
                        $this->headerStyle,
                    );
                }

                foreach ($encoder->encode($sheetRows) as $k => $cells) {
                    $manager->writeRow(
                        $sheetName,
                        $cells,
                        $this->resolveCellStyles($sheetRows->schema(), $cells, $indices[$k] + 1, $sheetName),
                    );
                }
            }
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
        if ($sheetName !== null && $this->sheetNameColumn !== null) {
            throw new InvalidArgumentException(
                'Cannot set both sheetName and sheetNameFromColumn. These options are mutually exclusive.',
            );
        }

        if ($sheetName !== null) {
            SheetNameAssertion::assert($sheetName);
        }

        $this->sheetName = $sheetName;

        return $this;
    }

    public function withSheetNameFromColumn(string $columnName): self
    {
        if ($this->sheetName !== null) {
            throw new InvalidArgumentException(
                'Cannot set both sheetName and sheetNameFromColumn. These options are mutually exclusive.',
            );
        }

        $this->sheetNameColumn = $columnName;

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

    /**
     * A serial write is the whole job and is admitted when it starts; a shipped task only stages its files.
     */
    private function files(): FileCommit
    {
        if ($this->files === null) {
            $this->files = new FileCommit(
                $this->filesystem,
                $this->path,
                $this->saveMode,
                $this->task->run ?? Part::serialRun(),
                $this->task->splits ?? 1,
                $this->task,
            );
            $this->admitted = $this->task !== null || $this->files->admit($this->router->partitioned());
        }

        return $this->files;
    }

    /**
     * A date cell carries no type of its own - the reader decides from the number format, so a column written
     * without one comes back as a number.
     *
     * @param Definition<mixed> $definition
     */
    private function temporalStyle(Definition $definition): ?Style
    {
        return match ($definition->type()::class) {
            DateTimeType::class
                => $this->formatStyles[$this->dateTimeFormat] ??= (new Style())->withFormat($this->dateTimeFormat),
            DateType::class => $this->formatStyles[$this->dateFormat] ??= (new Style())->withFormat($this->dateFormat),
            default => null,
        };
    }

    private function encoder(): ExcelEncoder
    {
        return $this->encoder ??= new ExcelEncoder(timeFormat: $this->timeFormat);
    }

    /**
     * @param array<int, null|bool|DateTimeInterface|float|int|string> $cells the row as the encoder wrote it
     *
     * @return null|array<int, null|Style>
     */
    private function resolveCellStyles(Schema $schema, array $cells, int $rowNumber, string $sheetName): ?array
    {
        $styles = [];
        $columnIndex = 0;

        foreach ($schema->definitions() as $definition) {
            $styles[$columnIndex] = $this->cellStyler?->style(
                $cells[$columnIndex],
                $definition,
                $rowNumber,
                $columnIndex,
                $sheetName,
            ) ?? $this->temporalStyle($definition);
            $columnIndex++;
        }

        return $styles;
    }

    private function resolveSheetName(mixed $value): string
    {
        if (is_string($value) && $value !== '') {
            SheetNameAssertion::assert($value);

            return $value;
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
