<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Native\String;

use function Flow\Types\DSL\{type_boolean, type_date, type_datetime, type_float, type_html, type_integer, type_json, type_null, type_string, type_time_zone, type_uuid, type_xml};
use Flow\Types\Type\Native\String\StringTypeNarrower;
use PHPUnit\Framework\TestCase;

final class StringTypeNarrowerTest extends TestCase
{
    public function test_detecting_boolean() : void
    {
        self::assertEquals(type_boolean(), StringTypeNarrower::narrow(type_string(), 'true'));
        self::assertEquals(type_boolean(), StringTypeNarrower::narrow(type_string(), 'false'));
        self::assertEquals(type_boolean(), StringTypeNarrower::narrow(type_string(), 'yes'));
        self::assertEquals(type_boolean(), StringTypeNarrower::narrow(type_string(), 'no'));
        self::assertEquals(type_boolean(), StringTypeNarrower::narrow(type_string(), 'on'));
        self::assertEquals(type_boolean(), StringTypeNarrower::narrow(type_string(), 'off'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow(type_string(), '0'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow(type_string(), 'not bool'));
    }

    public function test_detecting_date_time() : void
    {
        self::assertEquals(type_string(), StringTypeNarrower::narrow(type_string(), 'not date time'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow(type_string(), '2021-13-01'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow(type_string(), 'now'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow(type_string(), 'midnight'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow(type_string(), 'today'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow(type_string(), 'yesterday'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow(type_string(), 'tomorrow'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow(type_string(), '+24h'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow(type_string(), '00:00:00'));
        self::assertEquals(type_datetime(), StringTypeNarrower::narrow(type_string(), '2023-01-01 +10 hours'));
        self::assertEquals(type_datetime(), StringTypeNarrower::narrow(type_string(), 'Thursday, 02-Jun-2022 16:58:35 UTC'));
        self::assertEquals(type_datetime(), StringTypeNarrower::narrow(type_string(), '2022-06-02T16:58:35+0000'));
        self::assertEquals(type_datetime(), StringTypeNarrower::narrow(type_string(), '2022-06-02T16:58:35+00:00'));
        self::assertEquals(type_datetime(), StringTypeNarrower::narrow(type_string(), 'Thu, 02 Jun 22 16:58:35 +0000'));
        self::assertEquals(type_datetime(), StringTypeNarrower::narrow(type_string(), 'Thursday, 02-Jun-22 16:58:35 UTC'));
        self::assertEquals(type_datetime(), StringTypeNarrower::narrow(type_string(), 'Thu, 02 Jun 22 16:58:35 +0000'));
        self::assertEquals(type_datetime(), StringTypeNarrower::narrow(type_string(), 'Thu, 02 Jun 2022 16:58:35 +0000'));
    }

    public function test_detecting_float() : void
    {
        self::assertEquals(type_float(), StringTypeNarrower::narrow(type_string(), '1.0'));
        self::assertEquals(type_float(), StringTypeNarrower::narrow(type_string(), '2.1E-5'));
        self::assertEquals(type_float(), StringTypeNarrower::narrow(type_string(), '2.1e-5'));
        self::assertEquals(type_float(), StringTypeNarrower::narrow(type_string(), '0.0'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow(type_string(), 'not float'));
        self::assertEquals(type_integer(), StringTypeNarrower::narrow(type_string(), '1'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow(type_string(), '1.0.0'));
    }

    public function test_detecting_html() : void
    {
        self::assertEquals(type_html(), StringTypeNarrower::narrow(type_string(), '<html lang="en"><body><div><span>1</span></div></body></html>'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow(type_string(), 'not html'));
    }

    public function test_detecting_integer() : void
    {
        self::assertEquals(type_integer(), StringTypeNarrower::narrow(type_string(), '1'));
        self::assertEquals(type_integer(), StringTypeNarrower::narrow(type_string(), '0'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow(type_string(), 'not integer'));
        self::assertEquals(type_float(), StringTypeNarrower::narrow(type_string(), '1.0'));
        self::assertEquals(type_integer(), StringTypeNarrower::narrow(type_string(), '112312312'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow(type_string(), '11_2312_312'));
    }

    public function test_detecting_json() : void
    {
        self::assertEquals(type_json(), StringTypeNarrower::narrow(type_string(), '{"foo":"bar"}'));
        self::assertEquals(type_json(), StringTypeNarrower::narrow(type_string(), '[{"foo":"bar"}]'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow(type_string(), 'not json'));
    }

    public function test_detecting_null() : void
    {
        self::assertEquals(type_null(), StringTypeNarrower::narrow(type_string(), 'null'));
        self::assertEquals(type_null(), StringTypeNarrower::narrow(type_string(), 'NULL'));
        self::assertEquals(type_null(), StringTypeNarrower::narrow(type_string(), 'Nil'));
        self::assertEquals(type_null(), StringTypeNarrower::narrow(type_string(), 'nil'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow(type_string(), 'not null'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow(type_string(), ''));
    }

    public function test_detecting_timezone() : void
    {
        self::assertEquals(type_time_zone(), StringTypeNarrower::narrow(type_string(), 'UTC'));
        self::assertEquals(type_time_zone(), StringTypeNarrower::narrow(type_string(), 'America/New_York'));
        self::assertEquals(type_time_zone(), StringTypeNarrower::narrow(type_string(), 'Europe/London'));
        self::assertEquals(type_time_zone(), StringTypeNarrower::narrow(type_string(), 'Europe/Warsaw'));
        self::assertEquals(type_time_zone(), StringTypeNarrower::narrow(type_string(), 'Asia/Tokyo'));
        self::assertEquals(type_time_zone(), StringTypeNarrower::narrow(type_string(), 'Australia/Sydney'));

        self::assertEquals(type_time_zone(), StringTypeNarrower::narrow(type_string(), '+00:00'));
        self::assertEquals(type_time_zone(), StringTypeNarrower::narrow(type_string(), '+05:30'));
        self::assertEquals(type_time_zone(), StringTypeNarrower::narrow(type_string(), '-08:00'));

        // (military time zones not in official list - might cause a lot of false positives)
        self::assertEquals(type_string(), StringTypeNarrower::narrow(type_string(), 'A'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow(type_string(), 'B'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow(type_string(), 'Z'));

        self::assertEquals(type_string(), StringTypeNarrower::narrow(type_string(), 'PST'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow(type_string(), 'EST'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow(type_string(), 'CET'));

        self::assertEquals(type_string(), StringTypeNarrower::narrow(type_string(), 'not a timezone'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow(type_string(), 'Invalid/Timezone'));
        self::assertEquals(type_date(), StringTypeNarrower::narrow(type_string(), '2023-01-01'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow(type_string(), ''));
        self::assertEquals(type_integer(), StringTypeNarrower::narrow(type_string(), '123'));
    }

    public function test_detecting_uuid() : void
    {
        self::assertEquals(type_uuid(), StringTypeNarrower::narrow(type_string(), 'f47ac10b-58cc-4372-a567-0e02b2c3d479'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow(type_string(), 'not uuid'));
    }

    public function test_detecting_xml() : void
    {
        self::assertEquals(type_xml(), StringTypeNarrower::narrow(type_string(), '<foo>bar</foo>'));
        self::assertEquals(type_string(), StringTypeNarrower::narrow(type_string(), 'not xml'));
    }
}
