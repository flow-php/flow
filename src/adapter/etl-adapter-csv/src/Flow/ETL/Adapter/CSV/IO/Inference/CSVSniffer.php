<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\IO\Inference;

use Flow\ETL\Adapter\CSV\IO\Extractor\CSVSourceOpener;
use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\IO\Extractor\Grid\GridHeader;
use Flow\ETL\IO\Inference\InferredTypes;
use Flow\ETL\IO\Inference\Sample;
use Flow\ETL\IO\Inference\Sniffer;
use Flow\Types\Type\Native\String\StringTypeNarrower;
use SensitiveParameter;

use function array_map;
use function count;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_optional;
use function max;

final class CSVSniffer implements Describable, Sniffer
{
    private ?GridHeader $header;

    private readonly StringTypeNarrower $typer;

    /**
     * @param list<SourceFile> $sources materialised, not a Generator: header() and sniff() both index it
     * @param null|GridHeader $header the header already resolved by the parent sniffer; a one-unit sniffer never looks
     *                               for it again
     * @param null|FileSplit $window the byte window of its only source a part sniffs; null sniffs the sources whole
     */
    public function __construct(
        private readonly CSVSourceOpener $opener,
        private readonly array $sources,
        private readonly InferredTypes $candidates,
        ?GridHeader $header = null,
        private readonly ?FileSplit $window = null,
    ) {
        $this->header = $header;
        $this->typer = new StringTypeNarrower($candidates->toArray());
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        $sources = array_map(SourceFile::fromArray(...), type_list(type_array())->assert($data['sources'] ?? null));

        $header = type_optional(type_array())->assert($data['header'] ?? null);
        $window = type_optional(type_array())->assert($data['window'] ?? null);

        return new self(
            $rebuild->object(
                Description::fromArray(type_array()->assert($data['opener'] ?? null)),
                CSVSourceOpener::class,
            ),
            $sources,
            InferredTypes::fromArray(type_array()->assert($data['types'] ?? null)),
            $header === null ? null : GridHeader::fromArray($header),
            $window === null ? null : $rebuild->object(Description::fromArray($window), FileSplit::class),
        );
    }

    public function bytes(int $unit): int
    {
        return max(0, ($this->window === null ? $this->sources[$unit]->size : $this->window->length) ?? 0);
    }

    public function close(): void {}

    public function describe(): ?Description
    {
        $opener = $this->opener->describe();

        if ($opener === null) {
            return null;
        }

        $sources = array_map(static fn(SourceFile $source): array => $source->normalize(), $this->sources);

        return new Description(self::class, [
            'opener' => $opener,
            'sources' => $sources,
            'header' => $this->header?->normalize(),
            'types' => $this->candidates->normalize(),
            'window' => $this->window?->describe()->normalize(),
        ]);
    }

    /**
     * The first non-empty header among the sources, memoised; a unit sniffer inherits the parent's.
     */
    public function header(): GridHeader
    {
        if ($this->header !== null) {
            return $this->header;
        }

        foreach ($this->sources as $source) {
            $open = $this->opener->open($source);

            try {
                $columns = $open->columns();
            } finally {
                $open->close();
            }

            if ($columns !== []) {
                return $this->header = new GridHeader($columns, $source->uri());
            }
        }

        return $this->header = new GridHeader([], null);
    }

    public function names(): array
    {
        return $this->header()->names;
    }

    /**
     * Byte windows of the source when it is larger than $targetBytes, each resynced to a record start by the opener;
     * a source of unknown size, or one that fits, is one part.
     */
    public function parts(int $unit, int $targetBytes): array
    {
        $source = $this->sources[$unit];

        if ($source->size === null || $source->size <= $targetBytes) {
            return [new self($this->opener, [$source], $this->candidates, $this->header())];
        }

        $parts = [];

        foreach (FileSplit::windows($source, $targetBytes) as $window) {
            $parts[] = new self($this->opener, [$source], $this->candidates, $this->header(), $window);
        }

        return $parts;
    }

    public function sniff(int $unit, ?int $rowBudget): Sample
    {
        $open = $this->window === null
            ? $this->opener->open($this->sources[$unit])
            : $this->opener->openSplit($this->sources[$unit], $this->window);

        try {
            $columnTypes = $open->sniff($this->names(), $rowBudget, $this->typer);

            return new Sample(
                $columnTypes->folded(),
                $open->producedBytes(),
                $rowBudget === null || $columnTypes->rows() < $rowBudget,
            );
        } finally {
            $open->close();
        }
    }

    public function units(): int
    {
        return count($this->sources);
    }
}
