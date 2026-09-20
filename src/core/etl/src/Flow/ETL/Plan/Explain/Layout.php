<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Explain;

interface Layout
{
    public function render(Entry $root): string;
}
