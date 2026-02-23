<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit;

use Flow\Telemetry\ObjectExtractor;
use PHPUnit\Framework\TestCase;

final class ObjectExtractorTest extends TestCase
{
    public function test_all_uppercase_class() : void
    {
        self::assertSame('ABC', ObjectExtractor::shortName(new ABC()));
    }

    public function test_anonymous_class() : void
    {
        $anonymous = new class {};

        self::assertStringContainsString('class@anonymous', ObjectExtractor::shortName($anonymous));
    }

    public function test_consecutive_uppercase_letters() : void
    {
        self::assertSame('HTTPClient', ObjectExtractor::shortName(new HTTPClient()));
    }

    public function test_namespaced_class_extracts_short_name() : void
    {
        self::assertSame('ObjectExtractorTest', ObjectExtractor::shortName($this));
    }

    public function test_pascal_case_to_snake_case() : void
    {
        self::assertSame('ObjectExtractor', ObjectExtractor::shortName(new ObjectExtractor()));
    }

    public function test_simple_class_name() : void
    {
        self::assertSame('stdClass', ObjectExtractor::shortName(new \stdClass()));
    }

    public function test_single_letter_class() : void
    {
        self::assertSame('A', ObjectExtractor::shortName(new A()));
    }
}

class HTTPClient
{
}

class A
{
}

class ABC
{
}
