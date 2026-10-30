<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Logical;

enum Redefinition
{
    case names;
    case allBut;
    case unknown;
}
