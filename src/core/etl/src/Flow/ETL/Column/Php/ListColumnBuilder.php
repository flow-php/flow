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
        $this->element->appendPhysicals(array_values($physical));
        $this->offsets[] = $this->element->count();
    }

    public function appendPhysicals(array $physicals, ?int $nullCount = null): void
    {
        $elements = [];
        $end = $this->offsets[count($this->offsets) - 1];

        // @mago-ignore analysis:mixed-assignment
        foreach ($physicals as $physical) {
            if ($physical === null) {
                $this->nulls[count($this->offsets) - 1] = true;
                $this->offsets[] = $end;

                continue;
            }

            assert(is_array($physical));

            // @mago-ignore analysis:mixed-assignment
            foreach ($physical as $element) {
                $elements[] = $element;
            }

            $end += count($physical);
            $this->offsets[] = $end;
        }

        $this->element->appendPhysicals($elements);
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
