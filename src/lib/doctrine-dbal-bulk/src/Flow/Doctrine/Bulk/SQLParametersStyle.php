<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk;

enum SQLParametersStyle
{
    case NAMED;
    case POSITIONAL;
}
