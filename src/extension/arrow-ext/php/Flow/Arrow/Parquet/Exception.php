<?php

declare(strict_types=1);

namespace Flow\Arrow\Parquet;

if (\extension_loaded('arrow')) {
    return;
}

final class Exception extends \Exception {}
