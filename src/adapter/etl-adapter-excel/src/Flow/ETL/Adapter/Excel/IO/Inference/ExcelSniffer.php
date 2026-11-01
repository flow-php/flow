<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\IO\Inference;

use Flow\ETL\Adapter\Excel\IO\Extractor\WorkbookReader;
use Flow\ETL\Adapter\Excel\IO\Extractor\WorkbookSheet;
use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\IO\Extractor\Grid\GridHeader;
use Flow\ETL\IO\Inference\ColumnTypes;
use Flow\ETL\IO\Inference\InferredTypes;
use Flow\ETL\IO\Inference\Sample;
use Flow\ETL\IO\Inference\Sniffer;
use SensitiveParameter;

use function array_key_exists;
use function array_map;
use function count;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_optional;
use function max;

final class ExcelSniffer implements Describable, Sniffer
{
    private ?GridHeader $header;

    private readonly CellTypeNarrower $typer;

    /**
     * @var array<int, WorkbookSheet>
     */
    private array $sheets = [];

    /**
     * @param list<SourceFile> $files materialised, not a Generator: header() and sniff() both index it
     * @param null|GridHeader $header the header already resolved by the parent sniffer; a one-unit sniffer never
     *                                 looks for it again
     */
    public function __construct(
        private readonly WorkbookReader $workbook,
        private readonly array $files,
        private readonly InferredTypes $candidates,
        ?GridHeader $header = null,
    ) {
        $this->header = $header;
        $this->typer = new CellTypeNarrower($candidates);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        $files = array_map(SourceFile::fromArray(...), type_list(type_array())->assert($data['files'] ?? null));

        $header = type_optional(type_array())->assert($data['header'] ?? null);

        return new self(
            $rebuild->object(
                Description::fromArray(type_array()->assert($data['workbook'] ?? null)),
                WorkbookReader::class,
            ),
            $files,
            InferredTypes::fromArray(type_array()->assert($data['types'] ?? null)),
            $header === null ? null : GridHeader::fromArray($header),
        );
    }

    public function bytes(int $unit): int
    {
        return max(0, $this->files[$unit]->size ?? 0);
    }

    public function close(): void
    {
        foreach ($this->sheets as $sheet) {
            $sheet->close();
        }
    }

    public function describe(): ?Description
    {
        $workbook = $this->workbook->describe();

        if ($workbook === null) {
            return null;
        }

        $files = array_map(static fn(SourceFile $source): array => $source->normalize(), $this->files);

        return new Description(self::class, [
            'workbook' => $workbook,
            'files' => $files,
            'header' => $this->header?->normalize(),
            'types' => $this->candidates->normalize(),
        ]);
    }

    /**
     * Memoised: after a sniff the sampled sheets may be closed, and asking again would reopen one.
     */
    public function header(): GridHeader
    {
        if ($this->header !== null) {
            return $this->header;
        }

        foreach ($this->files as $index => $file) {
            $names = ($this->sheets[$index] ??= $this->workbook->sheet($file))->columns();

            if ($names !== []) {
                return $this->header = new GridHeader($names, $file->uri());
            }
        }

        return $this->header = new GridHeader([], null);
    }

    public function names(): array
    {
        return $this->header()->names;
    }

    /**
     * A bounded sample keeps what it parsed for take(); an unbounded one parses every row, so it streams and closes
     * as it goes. Either way the fold stops advancing a sheet once its budget is spent.
     */
    public function sniff(int $unit, ?int $rowBudget): Sample
    {
        $sheet = $this->sheets[$unit] ??= $this->workbook->sheet($this->files[$unit]);
        $sample = new WorkbookSheetSample($sheet, $rowBudget === null ? $sheet->rows() : $sheet->sample());

        return new Sample(
            ColumnTypes::sniff($this->names(), $sample, $rowBudget, $this->typer)->folded(),
            0,
            $sample->wholeSheet(),
        );
    }

    /**
     * The sheet this sampler read $file through, handed over to the caller to read on and close; null when the
     * sample never reached the file.
     */
    public function take(SourceFile $file): ?WorkbookSheet
    {
        foreach ($this->files as $index => $sampled) {
            if ($sampled->uri() === $file->uri() && array_key_exists($index, $this->sheets)) {
                $sheet = $this->sheets[$index];
                unset($this->sheets[$index]);

                return $sheet;
            }
        }

        return null;
    }

    public function parts(int $unit, int $targetBytes): array
    {
        return [new self($this->workbook, [$this->files[$unit]], $this->candidates, $this->header())];
    }

    public function units(): int
    {
        return count($this->files);
    }
}
