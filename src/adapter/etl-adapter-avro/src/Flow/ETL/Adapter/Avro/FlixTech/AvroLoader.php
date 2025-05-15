<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\FlixTech;

use Flow\ETL\{Exception\RuntimeException, FlowContext, Loader, Rows};
use Flow\ETL\Loader\{Closure, FileLoader};
use Flow\ETL\Schema;
use Flow\Filesystem\Path;

final readonly class AvroLoader implements Closure, FileLoader, Loader
{
    public function __construct(
        private Path $path,
        private ?Schema $schema = null,
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
