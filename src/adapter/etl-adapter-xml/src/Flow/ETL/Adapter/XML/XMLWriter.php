<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML;

use Flow\ETL\Adapter\XML\Abstraction\XMLNode;

interface XMLWriter
{
    public function write(XMLNode $node) : string;
}
