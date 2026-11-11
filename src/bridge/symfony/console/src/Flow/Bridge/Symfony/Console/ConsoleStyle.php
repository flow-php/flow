<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\Console;

enum ConsoleStyle
{
    case tasks;
    case graph;
    case line;
    case log;
}
