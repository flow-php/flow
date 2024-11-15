<?php

declare(strict_types=1);

namespace Flow\Dremel;

enum Repetition
{
    case OPTIONAL;
    case REPEATED;
    case REQUIRED;
}
