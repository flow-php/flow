<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\Tests\Unit\IO\Extractor\Pagination;

use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\SensitiveHeaders;
use Flow\ETL\Tests\FlowTestCase;

final class SensitiveHeadersTest extends FlowTestCase
{
    public function test_to_array_lists_the_name_of_every_header(): void
    {
        $headers = SensitiveHeaders::toArray();

        static::assertCount(count(SensitiveHeaders::cases()), $headers);
        static::assertContains('Authorization', $headers);
        static::assertContains('Proxy-Authorization', $headers);
        static::assertContains('Cookie', $headers);
        static::assertContains('X-Api-Key', $headers);
        static::assertContains('PRIVATE-TOKEN', $headers);
    }

    public function test_no_header_is_listed_twice_in_any_letter_case(): void
    {
        $lowercase = array_map(strtolower(...), SensitiveHeaders::toArray());

        static::assertSame($lowercase, array_values(array_unique($lowercase)));
    }
}
