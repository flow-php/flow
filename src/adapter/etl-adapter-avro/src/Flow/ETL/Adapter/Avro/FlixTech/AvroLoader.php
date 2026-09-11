<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\FlixTech;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Loader\FileLoader;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Option;
use Flow\Filesystem\Path\Option\ContentType;

final readonly class AvroLoader implements Closure, FileLoader, Loader
{
    private Path $path;

    public function __construct(
        Path $path,
        // @mago-ignore analysis:unused-property
        private ?Schema $schema = null,
        Filesystem $filesystem = new NativeLocalFilesystem(),
    ) {
        $this->path = $path->setOptionWhenEmpty(Option::CONTENT_TYPE->value, ContentType::AVRO);

        throw new RuntimeException(
            'Avro integration was abandoned due to lack of availability of good Avro libraries.',
        );
    }

    public function closure(FlowContext $context): void {}

    public function destination(): Path
    {
        return $this->path;
    }

    public function load(Rows $rows, FlowContext $context): void {}

    public function saveMode(SaveMode $mode): static
    {
        return $this;
    }
}
