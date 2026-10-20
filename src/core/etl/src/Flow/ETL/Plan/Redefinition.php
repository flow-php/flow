<?php

declare(strict_types=1);

namespace Flow\ETL\Plan;

enum Redefinition
{
    case names;
    case allBut;
    case unknown;
}
