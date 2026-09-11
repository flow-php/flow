<?php

declare(strict_types=1);

// The monorepo splits what a consumer gets from one autoloader: Flow's classes come from the root
// vendor, the Mago SDK from this tool's own. Loading both here keeps that split out of the shipped
// bin/worker.php, whose two-candidate lookup then finds nothing and leaves the classes to us.
require_once __DIR__ . '/../../vendor/autoload.php';
require_once __DIR__ . '/vendor/autoload.php';

require __DIR__ . '/../../src/bridge/mago/types/bin/worker.php';
