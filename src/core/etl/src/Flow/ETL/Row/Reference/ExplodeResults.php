<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference;

interface ExplodeResults
{
    public function explode() : bool;
}
