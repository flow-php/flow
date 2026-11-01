<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Explain;

interface Layout
{
    public function render(RenderedNode $root): string;
}
