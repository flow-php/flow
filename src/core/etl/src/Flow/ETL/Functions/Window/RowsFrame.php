<?php

declare(strict_types=1);

namespace Flow\ETL\Functions\Window;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Values\Rows;
use SensitiveParameter;

use function Flow\Types\DSL\type_array;
use function max;
use function min;

final readonly class RowsFrame implements WindowFrame
{
    public function __construct(
        private FrameBound $start,
        private FrameBound $end,
    ) {}

    public function bounds(int $index, Rows $partition): array
    {
        $lastIndex = $partition->count() - 1;

        $start = $this->start->resolve($index, $lastIndex);
        $end = $this->end->resolve($index, $lastIndex);

        if ($start > $lastIndex || $end < 0 || $start > $end) {
            return [1, 0];
        }

        return [max(0, $start), min($lastIndex, $end)];
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            FrameBound::fromArray(type_array()->assert($data['start'] ?? null)),
            FrameBound::fromArray(type_array()->assert($data['end'] ?? null)),
        );
    }

    public function describe(): ?Description
    {
        return new Description(self::class, ['start' => $this->start->normalize(), 'end' => $this->end->normalize()]);
    }
}
