<?php

declare(strict_types=1);

namespace Flow\Arrow\Parquet;

use Exception as BaseException;

use function extension_loaded;

if (extension_loaded('arrow')) {
    return;
}

final class Exception extends BaseException {}
