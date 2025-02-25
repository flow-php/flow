<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation;

use Flow\Bridge\Symfony\HttpFoundation\Output\Type;
use Flow\ETL\Loader;

interface Output
{
    public function memoryLoader(string $id) : Loader;

    public function stdoutLoader() : Loader;

    public function type() : Type;
}
