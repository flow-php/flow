<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Rows;
use Flow\ETL\Schema;

interface Hydrator
{
    /**
     * @param list<RawRowValues> $batch
     */
    public function cast(array $batch, ?Schema $schema = null): Rows;

    /**
     * @return list<TypedRowValues>
     */
    public function dehydrate(Rows $rows): array;

    /**
     * @param list<RawRowValues> $batch
     */
    public function hydrate(array $batch, ?Schema $schema = null): Rows;
}
