<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\IO\Extractor;

use Flow\ETL\Adapter\Excel\IO\Extractor\Sheet\OpenSheet;
use Flow\ETL\Adapter\Excel\IO\Extractor\Sheet\SheetCells;
use Flow\ETL\Adapter\Excel\IO\Extractor\Sheet\SheetsManager;
use Flow\ETL\Foundation\Describe\ConfiguredFilesystem;
use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\IO\Extractor\Grid\RecordDecoder;
use Flow\Filesystem\Path;
use OpenSpout\Reader\ODS\Reader as OdsReader;
use OpenSpout\Reader\XLSX\Reader as XlsxReader;
use SensitiveParameter;
use Throwable;

use function Flow\Types\DSL\type_array;

final readonly class WorkbookReader implements Describable
{
    /**
     * The extensions a workbook is read with: this package's composer.json and openspout's.
     */
    public const array EXTENSIONS = ['dom', 'filter', 'xmlreader', 'zip'];

    public function __construct(
        private ExcelReadOptions $options,
        private ExcelFormatDetector $detector,
    ) {}

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            ExcelReadOptions::fromArray(type_array()->assert($data['options'] ?? null)),
            new ExcelFormatDetector(
                $rebuild->of($data['filesystem'] ?? null, ConfiguredFilesystem::class)->filesystem(),
            ),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(
            self::class,
            [
                'options' => $this->options->normalize(),
                'filesystem' => new ConfiguredFilesystem($this->detector->filesystem()),
            ],
            extensions: self::EXTENSIONS,
        );
    }

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
            (new SheetCells($sheet, $this->options->withHeader))->rows(),
            new RecordDecoder(
                withHeader: $this->options->withHeader,
                emptyToNull: $this->options->convertEmptyToNull,
                missingColumnsAsNull: $this->options->missingColumnsAsNull,
            ),
        );
    }

    public function sheet(SourceFile $source): WorkbookSheet
    {
        return new WorkbookSheet($source->path, $this);
    }
}
