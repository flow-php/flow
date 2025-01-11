<?php

declare(strict_types=1);

namespace Flow\Documentation\Manifest;

enum Type : string
{
    case ADAPTER = 'adapter';
    case BRIDGE = 'bridge';
    case CLI = 'cli';
    case CORE = 'core';
    case LIB = 'lib';
}
