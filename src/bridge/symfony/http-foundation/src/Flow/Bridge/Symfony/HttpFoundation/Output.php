<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation;

use Flow\Bridge\Symfony\HttpFoundation\Output\Type;
use Flow\ETL\Loader;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;

interface Output
{
    public function loader(Path $path, Filesystem $filesystem): Loader;

    public function type(): Type;
}
