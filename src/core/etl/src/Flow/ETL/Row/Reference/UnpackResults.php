<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference;

interface UnpackResults
{
    public function unpack() : bool;
}
