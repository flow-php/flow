<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Pipeline\Element;

interface Pipeline
{
    public function register(Element $element) : void;

    /**
     * @param \Generator<int, Rows, mixed, void> $generator
     */
    public function process(\Generator $generator) : void;
}
