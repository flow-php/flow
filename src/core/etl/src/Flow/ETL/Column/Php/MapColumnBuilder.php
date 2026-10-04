<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Php;

use Flow\ETL\Column\Column;
use Flow\Types\Type;

use function array_keys;
use function array_values;
use function assert;
use function count;
use function is_array;

final class MapColumnBuilder implements PhpColumnBuilder
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
        private readonly PhpColumnBuilder $keys,
        private readonly PhpColumnBuilder $values,
    ) {}

    public function appendPhysical(mixed $physical): void
    {
        if ($physical === null) {
            $this->nulls[count($this->offsets) - 1] = true;
            $this->offsets[] = $this->offsets[count($this->offsets) - 1];

            return;
        }

        assert(is_array($physical));
        $this->keys->appendPhysicals(array_keys($physical));
        $this->values->appendPhysicals(array_values($physical));
        $this->offsets[] = $this->keys->count();
    }

    public function appendPhysicals(array $physicals, ?int $nullCount = null): void
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
        return new MapColumn($this->type, $this->offsets, $this->keys->finish(), $this->values->finish(), $this->nulls);
    }
}
