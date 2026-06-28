<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Runtime;

enum RuntimeMode: string
{
    case Auto = 'auto';
    case Classic = 'classic';
    case Worker = 'worker';
}
