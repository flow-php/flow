<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Factory;

use Flow\ETL\Row\Factory\StringTypeChecker;
use PHPUnit\Framework\TestCase;

final class StringTypeCheckerTest extends TestCase
{
    public function test_detecting_boolean() : void
    {
        $this->assertTrue((new StringTypeChecker('true'))->isBoolean());
        $this->assertTrue((new StringTypeChecker('false'))->isBoolean());
        $this->assertFalse((new StringTypeChecker('0'))->isBoolean());
        $this->assertFalse((new StringTypeChecker('not bool'))->isBoolean());
    }

    public function test_detecting_date_time() : void
    {
        $this->assertFalse((new StringTypeChecker('not date time'))->isDateTime());
        $this->assertFalse((new StringTypeChecker('2021-13-01'))->isDateTime());
        $this->assertFalse((new StringTypeChecker('now'))->isDateTime());
        $this->assertFalse((new StringTypeChecker('midnight'))->isDateTime());
        $this->assertFalse((new StringTypeChecker('today'))->isDateTime());
        $this->assertFalse((new StringTypeChecker('yesterday'))->isDateTime());
        $this->assertFalse((new StringTypeChecker('tomorrow'))->isDateTime());
        $this->assertFalse((new StringTypeChecker('+24h'))->isDateTime());
        $this->assertFalse((new StringTypeChecker('00:00:00'))->isDateTime());
        $this->assertTrue((new StringTypeChecker('2023-01-01 +10 hours'))->isDateTime());
        $this->assertTrue((new StringTypeChecker('Thursday, 02-Jun-2022 16:58:35 UTC'))->isDateTime());
        $this->assertTrue((new StringTypeChecker('2022-06-02T16:58:35+0000'))->isDateTime());
        $this->assertTrue((new StringTypeChecker('2022-06-02T16:58:35+00:00'))->isDateTime());
        $this->assertTrue((new StringTypeChecker('Thu, 02 Jun 22 16:58:35 +0000'))->isDateTime());
        $this->assertTrue((new StringTypeChecker('Thursday, 02-Jun-22 16:58:35 UTC'))->isDateTime());
        $this->assertTrue((new StringTypeChecker('Thu, 02 Jun 22 16:58:35 +0000'))->isDateTime());
        $this->assertTrue((new StringTypeChecker('Thu, 02 Jun 2022 16:58:35 +0000'))->isDateTime());
    }

    public function test_detecting_float() : void
    {
        $this->assertTrue((new StringTypeChecker('1.0'))->isFloat());
        $this->assertTrue((new StringTypeChecker('0.0'))->isFloat());
        $this->assertFalse((new StringTypeChecker('not float'))->isFloat());
        $this->assertFalse((new StringTypeChecker('1'))->isFloat());
        $this->assertFalse((new StringTypeChecker('1.0.0'))->isFloat());
    }

    public function test_detecting_integer() : void
    {
        $this->assertTrue((new StringTypeChecker('1'))->isInteger());
        $this->assertTrue((new StringTypeChecker('0'))->isInteger());
        $this->assertFalse((new StringTypeChecker('not integer'))->isInteger());
        $this->assertFalse((new StringTypeChecker('1.0'))->isInteger());
        $this->assertTrue((new StringTypeChecker('112312312'))->isInteger());
        $this->assertFalse((new StringTypeChecker('11_2312_312'))->isInteger());
    }

    public function test_detecting_json() : void
    {
        $this->assertTrue((new StringTypeChecker('{"foo":"bar"}'))->isJson());
        $this->assertTrue((new StringTypeChecker('[{"foo":"bar"}]'))->isJson());
        $this->assertFalse((new StringTypeChecker('not json'))->isJson());
    }

    public function test_detecting_null() : void
    {
        $this->assertTrue((new StringTypeChecker('null'))->isNull());
        $this->assertTrue((new StringTypeChecker('NULL'))->isNull());
        $this->assertTrue((new StringTypeChecker('Nil'))->isNull());
        $this->assertTrue((new StringTypeChecker('nil'))->isNull());
        $this->assertFalse((new StringTypeChecker('not null'))->isNull());
        $this->assertFalse((new StringTypeChecker(''))->isNull());
    }

    public function test_detecting_uuid() : void
    {
        $this->assertTrue((new StringTypeChecker('f47ac10b-58cc-4372-a567-0e02b2c3d479'))->isUuid());
        $this->assertFalse((new StringTypeChecker('not uuid'))->isUuid());
    }

    public function test_detecting_xml() : void
    {
        $this->assertTrue((new StringTypeChecker('<foo>bar</foo>'))->isXML());
        $this->assertFalse((new StringTypeChecker('not xml'))->isXML());
    }
}
