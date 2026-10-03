<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Php;

use Flow\ETL\Column\Column;
use Flow\ETL\Column\Physical\Physical;
use Flow\Types\Type;

use function array_push;
use function assert;
use function count;
use function is_array;
use function is_object;

final class ScalarColumnBuilder implements PhpColumnBuilder
{
    private int $nullCount = 0;

    /**
     * @var list<mixed>
     */
    private array $values = [];

    /**
     * @param Type<mixed> $type
     */
    public function __construct(
        private readonly Type $type,
        private readonly Physical $physical,
    ) {}

    public function appendPhysical(mixed $physical): void
    {
        assert(!is_array($physical) && !is_object($physical));

        if ($physical === null) {
            $this->nullCount++;
        }

        $this->values[] = $physical;
    }

    public function appendPhysicals(array $physicals, ?int $nullCount = null): void
    {
        $this->nullCount += $nullCount ?? count(array_keys($physicals, null, true));
        array_push($this->values, ...$physicals);
    }

    public function count(): int
    {
        return count($this->values);
    }

    public function finish(): Column
    {
        return new ScalarColumn($this->type, $this->physical, $this->values, $this->nullCount);
    }
}
