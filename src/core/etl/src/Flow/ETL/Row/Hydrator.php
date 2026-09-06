<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Rows;
use Flow\ETL\Schema;

interface Hydrator
{
    /**
     * @return list<TypedRowValues>
     */
    public function dehydrate(Rows $rows): array;

    /**
     * @param list<RawRowValues> $batch
     *
     * @throws SchemaMismatchException
     */
    public function hydrate(array $batch, Schema $schema): Rows;
}
