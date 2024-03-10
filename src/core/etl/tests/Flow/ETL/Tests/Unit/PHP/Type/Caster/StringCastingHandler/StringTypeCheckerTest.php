<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Caster\StringCastingHandler;

use Flow\ETL\PHP\Type\Caster\StringCastingHandler\StringTypeChecker;
use PHPUnit\Framework\TestCase;

final class StringTypeCheckerTest extends TestCase
{
    public function test_detecting_boolean() : void
    {
        self::assertTrue((new StringTypeChecker('true'))->isBoolean());
        self::assertTrue((new StringTypeChecker('false'))->isBoolean());
        self::assertTrue((new StringTypeChecker('yes'))->isBoolean());
        self::assertTrue((new StringTypeChecker('no'))->isBoolean());
        self::assertTrue((new StringTypeChecker('on'))->isBoolean());
        self::assertTrue((new StringTypeChecker('off'))->isBoolean());
        self::assertFalse((new StringTypeChecker('0'))->isBoolean());
        self::assertFalse((new StringTypeChecker('not bool'))->isBoolean());
    }

    public function test_detecting_date_time() : void
    {
        self::assertFalse((new StringTypeChecker('not date time'))->isDateTime());
        self::assertFalse((new StringTypeChecker('2021-13-01'))->isDateTime());
        self::assertFalse((new StringTypeChecker('now'))->isDateTime());
        self::assertFalse((new StringTypeChecker('midnight'))->isDateTime());
        self::assertFalse((new StringTypeChecker('today'))->isDateTime());
        self::assertFalse((new StringTypeChecker('yesterday'))->isDateTime());
        self::assertFalse((new StringTypeChecker('tomorrow'))->isDateTime());
        self::assertFalse((new StringTypeChecker('+24h'))->isDateTime());
        self::assertFalse((new StringTypeChecker('00:00:00'))->isDateTime());
        self::assertTrue((new StringTypeChecker('2023-01-01 +10 hours'))->isDateTime());
        self::assertTrue((new StringTypeChecker('Thursday, 02-Jun-2022 16:58:35 UTC'))->isDateTime());
        self::assertTrue((new StringTypeChecker('2022-06-02T16:58:35+0000'))->isDateTime());
        self::assertTrue((new StringTypeChecker('2022-06-02T16:58:35+00:00'))->isDateTime());
        self::assertTrue((new StringTypeChecker('Thu, 02 Jun 22 16:58:35 +0000'))->isDateTime());
        self::assertTrue((new StringTypeChecker('Thursday, 02-Jun-22 16:58:35 UTC'))->isDateTime());
        self::assertTrue((new StringTypeChecker('Thu, 02 Jun 22 16:58:35 +0000'))->isDateTime());
        self::assertTrue((new StringTypeChecker('Thu, 02 Jun 2022 16:58:35 +0000'))->isDateTime());
    }

    public function test_detecting_float() : void
    {
        self::assertTrue((new StringTypeChecker('1.0'))->isFloat());
        self::assertTrue((new StringTypeChecker('0.0'))->isFloat());
        self::assertFalse((new StringTypeChecker('not float'))->isFloat());
        self::assertFalse((new StringTypeChecker('1'))->isFloat());
        self::assertFalse((new StringTypeChecker('1.0.0'))->isFloat());
    }

    public function test_detecting_integer() : void
    {
        self::assertTrue((new StringTypeChecker('1'))->isInteger());
        self::assertTrue((new StringTypeChecker('0'))->isInteger());
        self::assertFalse((new StringTypeChecker('not integer'))->isInteger());
        self::assertFalse((new StringTypeChecker('1.0'))->isInteger());
        self::assertTrue((new StringTypeChecker('112312312'))->isInteger());
        self::assertFalse((new StringTypeChecker('11_2312_312'))->isInteger());
    }

    public function test_detecting_json() : void
    {
        self::assertTrue((new StringTypeChecker('{"foo":"bar"}'))->isJson());
        self::assertTrue((new StringTypeChecker('[{"foo":"bar"}]'))->isJson());
        self::assertFalse((new StringTypeChecker('not json'))->isJson());
    }

    public function test_detecting_null() : void
    {
        self::assertTrue((new StringTypeChecker('null'))->isNull());
        self::assertTrue((new StringTypeChecker('NULL'))->isNull());
        self::assertTrue((new StringTypeChecker('Nil'))->isNull());
        self::assertTrue((new StringTypeChecker('nil'))->isNull());
        self::assertFalse((new StringTypeChecker('not null'))->isNull());
        self::assertFalse((new StringTypeChecker(''))->isNull());
    }

    public function test_detecting_uuid() : void
    {
        self::assertTrue((new StringTypeChecker('f47ac10b-58cc-4372-a567-0e02b2c3d479'))->isUuid());
        self::assertFalse((new StringTypeChecker('not uuid'))->isUuid());
    }

    public function test_detecting_xml() : void
    {
        self::assertTrue((new StringTypeChecker('<foo>bar</foo>'))->isXML());
        self::assertFalse((new StringTypeChecker('not xml'))->isXML());
    }
}
