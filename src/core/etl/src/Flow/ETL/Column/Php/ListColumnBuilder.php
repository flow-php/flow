<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Php;

use Flow\ETL\Column\Column;
use Flow\Types\Type;

use function array_values;
use function assert;
use function count;
use function is_array;

final class ListColumnBuilder implements PhpColumnBuilder
{
    /**
     * @var array<int, true>
     */
    private array $nulls = [];

    /**
     * @var list<int>
     */
    private array $offsets = [0];

    /**
     * @param Type<mixed> $type
     */
    public function __construct(
        private readonly Type $type,
        private readonly PhpColumnBuilder $element,
    ) {}

    public function appendPhysical(mixed $physical): void
    {
        if ($physical === null) {
            $this->nulls[count($this->offsets) - 1] = true;
            $this->offsets[] = $this->offsets[count($this->offsets) - 1];

            return;
        }

        assert(is_array($physical));
        $this->element->appendPhysicalMany(array_values($physical));
        $this->offsets[] = $this->element->count();
    }

    public function appendPhysicalMany(array $physicals, ?int $nullCount = null): void
    {
        // @mago-ignore analysis:mixed-assignment
        foreach ($physicals as $physical) {
            $this->appendPhysical($physical);
        }
    }

    public function count(): int
    {
        return count($this->offsets) - 1;
    }

    public function finish(): Column
    {
        return new ListColumn($this->type, $this->offsets, $this->element->finish(), $this->nulls);
    }
}
