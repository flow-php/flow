<?php

declare(strict_types=1);

namespace Flow\ETL;

interface Loader
{
    public function load(Rows $rows, FlowContext $context) : void;
}
