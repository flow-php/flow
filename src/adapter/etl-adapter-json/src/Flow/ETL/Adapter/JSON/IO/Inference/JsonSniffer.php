<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\IO\Inference;

use Flow\ETL\Adapter\JSON\IO\Extractor\JsonFileReader;
use Flow\ETL\Adapter\JSON\IO\JsonFraming;
use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\IO\Inference\ColumnTypes;
use Flow\ETL\IO\Inference\Sample;
use Flow\ETL\IO\Inference\Sniffer;
use Flow\Types\Type\Logical\InstanceOfTypeNarrower;
use SensitiveParameter;

use function array_map;
use function count;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_optional;
use function max;

final readonly class JsonSniffer implements Describable, Sniffer
{
    /**
     * @param list<SourceFile> $sources the extractor's materialised listing, already filtered, so filesToSniff
     *                                  counts only files the read would open
     * @param null|FileSplit $window the byte window of its only source a part sniffs, Lines only; null sniffs the
     *                               sources whole
     */
    public function __construct(
        private JsonFileReader $reader,
        private array $sources,
        private ?FileSplit $window = null,
    ) {}

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        $sources = array_map(SourceFile::fromArray(...), type_list(type_array())->assert($data['sources'] ?? null));

        $window = type_optional(type_array())->assert($data['window'] ?? null);

        return new self(
            $rebuild->object(
                Description::fromArray(type_array()->assert($data['reader'] ?? null)),
                JsonFileReader::class,
            ),
            $sources,
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
        $reader = $this->reader->describe();

        if ($reader === null) {
            return null;
        }

        $sources = array_map(static fn(SourceFile $source): array => $source->normalize(), $this->sources);

        return new Description(self::class, [
            'reader' => $reader,
            'sources' => $sources,
            'window' => $this->window?->describe()->normalize(),
        ]);
    }

    public function names(): array
    {
        return [];
    }

    public function sniff(int $unit, ?int $rowBudget): Sample
    {
        $sample = new JsonFileSample($this->reader, $this->sources[$unit], $this->window);
        $columnTypes = ColumnTypes::sniff([], $sample, $rowBudget, new InstanceOfTypeNarrower());

        return new Sample($columnTypes->folded(), max(0, $sample->bytes()), $sample->complete());
    }

    /**
     * Byte windows of a JSON lines source larger than $targetBytes, each resynced to a line start by the reader; a
     * document, a source of unknown size or one that fits is one part.
     */
    public function parts(int $unit, int $targetBytes): array
    {
        $source = $this->sources[$unit];

        if ($this->reader->format !== JsonFraming::Lines || $source->size === null || $source->size <= $targetBytes) {
            return [new self($this->reader, [$source])];
        }

        $parts = [];

        foreach (FileSplit::windows($source, $targetBytes) as $window) {
            $parts[] = new self($this->reader, [$source], $window);
        }

        return $parts;
    }

    public function units(): int
    {
        return count($this->sources);
    }
}
