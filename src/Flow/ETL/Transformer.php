<?php

declare(strict_types=1);

namespace Flow\ETL;

/**
 * @psalm-immutable
 */
interface Transformer
{
    public function transform(Rows $rows) : Rows;
}
