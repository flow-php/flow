<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS\Tests\Unit\DSL;

use Flow\Filesystem\Bridge\AsyncAWS\Options;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\Bridge\AsyncAWS\DSL\aws_s3_filesystem_options;

final class FunctionsTest extends TestCase
{
    public function test_aws_s3_filesystem_options_are_the_default_options(): void
    {
        static::assertEquals(new Options(), aws_s3_filesystem_options());
    }
}
