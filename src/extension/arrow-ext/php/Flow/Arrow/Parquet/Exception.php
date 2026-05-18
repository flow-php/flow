<?php

declare(strict_types=1);

namespace Flow\Arrow\Parquet;

use function extension_loaded;

if (extension_loaded('arrow')) {
    return;
}

final class Exception extends \Exception {}
