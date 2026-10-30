<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\IO\Loader;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\IO\Loader;
use Flow\ETL\IO\Loader\Closure;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use SensitiveParameter;

final readonly class AvroLoader implements Closure, Loader
{
    public function __construct(
        Path $path,
        ?Schema $schema = null,
        Filesystem $filesystem = new NativeLocalFilesystem(),
    ) {
        throw new RuntimeException(
            'Avro integration was abandoned due to lack of availability of good Avro libraries.',
        );
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
    }

    public function describe(): ?Description
    {
        return null;
    }

    public function closure(): void {}

    public function load(Rows $rows, Backend $backend): void {}
}
