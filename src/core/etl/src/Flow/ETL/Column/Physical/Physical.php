<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Physical;

interface Physical
{
    /**
     * @param mixed $value already cast by the column's Type, never null
     */
    public function toPhysical(mixed $value): mixed;

    /**
     * @param mixed $physical never null
     */
    public function fromPhysical(mixed $physical): mixed;

    /**
     * @param list<mixed> $physicals nulls stay null
     *
     * @return list<mixed>
     */
    public function fromPhysicalAll(array $physicals): array;
}
