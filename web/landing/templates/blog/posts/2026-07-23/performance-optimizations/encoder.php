<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

/**
 * @template TPhysical
 */
interface Encoder
{
    /**
     * @param list<TypedRowValues> $batch
     *
     * @return list<TPhysical>
     */
    public function encode(array $batch): array;

    /**
     * @param list<TPhysical> $batch
     *
     * @return list<RawRowValues>
     */
    public function decode(array $batch): array;
}
