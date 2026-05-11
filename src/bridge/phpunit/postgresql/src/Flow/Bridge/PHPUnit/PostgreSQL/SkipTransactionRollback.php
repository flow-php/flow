<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\PostgreSQL;

#[\Attribute(\Attribute::TARGET_CLASS | \Attribute::TARGET_METHOD)]
final class SkipTransactionRollback {}
