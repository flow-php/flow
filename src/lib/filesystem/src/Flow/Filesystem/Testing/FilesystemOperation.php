<?php

declare(strict_types=1);

namespace Flow\Filesystem\Testing;

enum FilesystemOperation
{
    case AppendTo;
    case Create;
    case List;
    case Mv;
    case ReadFrom;
    case Rm;
    case Status;
    case WriteTo;
}
