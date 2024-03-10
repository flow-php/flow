<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Filesystem\Tests\Integration;

use Flow\ETL\Adapter\Filesystem\{AwsS3Stream, AzureBlobStream};
use PHPUnit\Framework\TestCase;

final class RegisterWrapperTest extends TestCase
{
    protected function tearDown() : void
    {
        if (\in_array('flow-aws-s3', \stream_get_wrappers(), true)) {
            \stream_wrapper_unregister('flow-aws-s3');
        }

        if (\in_array('flow-azure-blob', \stream_get_wrappers(), true)) {
            \stream_wrapper_unregister('flow-azure-blob');
        }
    }

    public function test_registering_wrappers() : void
    {
        self::assertFalse(\in_array('flow-aws-s3', \stream_get_wrappers(), true));
        self::assertFalse(\in_array('flow-azure-blob', \stream_get_wrappers(), true));

        AwsS3Stream::register();
        AzureBlobStream::register();

        self::assertTrue(\in_array('flow-aws-s3', \stream_get_wrappers(), true));
        self::assertTrue(\in_array('flow-azure-blob', \stream_get_wrappers(), true));
    }
}
