<?php

declare(strict_types=1);

namespace Flow\Floe\Exception;

use Exception as BaseException;

use function extension_loaded;

if (extension_loaded('flow_php')) {
    return;
}

final class ExtensionException extends BaseException {}
