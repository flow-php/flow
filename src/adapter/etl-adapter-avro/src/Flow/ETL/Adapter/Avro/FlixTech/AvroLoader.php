<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\FlixTech;

use Flow\ETL\Filesystem\Path;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Row\Schema;
use Flow\ETL\{Exception\RuntimeException, FlowContext, Loader, Rows};

final class AvroLoader implements Closure, Loader, Loader\FileLoader
{
    public function __construct(
        private readonly Path $path,
        private readonly ?Schema $schema = null
    ) {
        throw new RuntimeException('Avro integration was abandoned due to lack of availability of good Avro libraries.');
    }

    public function closure(FlowContext $context) : void
    {
    }

    public function destination() : Path
    {
        return $this->path;
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
    }
}
