<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\PostgreSQL;

use Attribute;

#[Attribute(Attribute::TARGET_CLASS | Attribute::TARGET_METHOD)]
final class SkipTransactionRollback {}
