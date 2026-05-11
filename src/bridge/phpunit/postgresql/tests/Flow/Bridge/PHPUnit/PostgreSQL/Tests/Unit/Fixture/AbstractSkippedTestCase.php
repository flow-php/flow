<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\PostgreSQL\Tests\Unit\Fixture;

use Flow\Bridge\PHPUnit\PostgreSQL\SkipTransactionRollback;
use PHPUnit\Framework\TestCase;

#[SkipTransactionRollback]
abstract class AbstractSkippedTestCase extends TestCase {}
