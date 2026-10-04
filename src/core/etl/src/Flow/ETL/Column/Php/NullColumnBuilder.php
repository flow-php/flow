<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Php;

use Flow\ETL\Column\Column;
use Flow\ETL\Column\Physical\NullPhysical;
use Flow\Types\Type;

use function count;

final class NullColumnBuilder implements PhpColumnBuilder
{
    private int $count = 0;

    /**
     * @param Type<mixed> $type
     */
    public function __construct(
        private readonly Type $type,
    ) {}

    public function appendPhysical(mixed $physical): void
    {
        $this->count++;
    }

    public function appendPhysicals(array $physicals, ?int $nullCount = null): void
    {
        $this->count += count($physicals);
    }

    public function count(): int
    {
        return $this->count;
    }

    public function finish(): Column
    {
        return new ConstantColumn($this->type, new NullPhysical(), null, $this->count);
    }
}
