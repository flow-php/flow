<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation;

use Flow\Bridge\Symfony\HttpFoundation\Output\Type;
use Flow\ETL\Loader;

interface Output
{
    public function loader() : Loader;

    public function type() : Type;
}
