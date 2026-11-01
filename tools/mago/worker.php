<?php

declare(strict_types=1);

// Loads Flow from the root vendor and the Mago SDK from this tool's vendor, keeping that split out of bin/worker.php.
require_once __DIR__ . '/../../vendor/autoload.php';
require_once __DIR__ . '/vendor/autoload.php';

require __DIR__ . '/../../src/bridge/mago/types/bin/worker.php';
