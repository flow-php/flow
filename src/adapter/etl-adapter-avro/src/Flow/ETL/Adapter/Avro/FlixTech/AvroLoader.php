<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\FlixTech;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Loader\FileLoader;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Option;
use Flow\Filesystem\Path\Option\ContentType;

final readonly class AvroLoader implements Closure, FileLoader, Loader
{
    private Path $path;

    public function __construct(
        Path $path,
        private ?Schema $schema = null,
    ) {
        throw new RuntimeException(
            'Avro integration was abandoned due to lack of availability of good Avro libraries.',
        );
        $this->path = $path->setOptionWhenEmpty(Option::CONTENT_TYPE->value, ContentType::AVRO);
    }

    public function closure(FlowContext $context): void {}

    public function destination(): Path
    {
        return $this->path;
    }

    public function load(Rows $rows, FlowContext $context): void {}
}
