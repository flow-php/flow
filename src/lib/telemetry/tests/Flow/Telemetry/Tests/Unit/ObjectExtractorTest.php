<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit;

use Flow\Telemetry\ObjectExtractor;
use PHPUnit\Framework\TestCase;
use stdClass;

final class ObjectExtractorTest extends TestCase
{
    public function test_all_uppercase_class(): void
    {
        static::assertSame('ABC', ObjectExtractor::shortName(new ABC()));
    }

    public function test_anonymous_class(): void
    {
        $anonymous = new class {};

        static::assertStringContainsString('class@anonymous', ObjectExtractor::shortName($anonymous));
    }

    public function test_consecutive_uppercase_letters(): void
    {
        static::assertSame('HTTPClient', ObjectExtractor::shortName(new HTTPClient()));
    }

    public function test_namespaced_class_extracts_short_name(): void
    {
        static::assertSame('ObjectExtractorTest', ObjectExtractor::shortName($this));
    }

    public function test_pascal_case_to_snake_case(): void
    {
        static::assertSame('ObjectExtractor', ObjectExtractor::shortName(new ObjectExtractor()));
    }

    public function test_simple_class_name(): void
    {
        static::assertSame('stdClass', ObjectExtractor::shortName(new stdClass()));
    }

    public function test_single_letter_class(): void
    {
        static::assertSame('A', ObjectExtractor::shortName(new A()));
    }
}

class HTTPClient {}

class A {}

class ABC {}
