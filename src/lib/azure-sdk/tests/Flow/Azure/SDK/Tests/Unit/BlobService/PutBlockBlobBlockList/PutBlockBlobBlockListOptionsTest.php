<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\Tests\Unit\BlobService\PutBlockBlobBlockList;

use Flow\Azure\SDK\BlobService\PutBlockBlobBlockList\PutBlockBlobBlockListOptions;
use PHPUnit\Framework\TestCase;

use function array_key_exists;

final class PutBlockBlobBlockListOptionsTest extends TestCase
{
    public function test_if_none_match_header_is_absent_by_default(): void
    {
        static::assertFalse(array_key_exists('If-None-Match', (new PutBlockBlobBlockListOptions())->toHeaders()));
    }

    public function test_if_none_match_header_is_sent_when_set(): void
    {
        static::assertSame(
            '*',
            (new PutBlockBlobBlockListOptions())
                ->withIfNoneMatch('*')
                ->toHeaders()['If-None-Match'],
        );
    }
}
