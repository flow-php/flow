<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Filesystem;

use Flow\ETL\Filesystem\AwsS3Stream;
use Flow\ETL\Filesystem\AzureBlobStream;
use PHPUnit\Framework\TestCase;

final class RegisterWrapperTest extends TestCase
{
    public function test_registering_wrappers() : void
    {
        $this->assertFalse(\in_array('flow-aws-s3', \stream_get_wrappers(), true));
        $this->assertFalse(\in_array('flow-azure-blob', \stream_get_wrappers(), true));

        AwsS3Stream::register();
        AzureBlobStream::register();

        $this->assertTrue(\in_array('flow-aws-s3', \stream_get_wrappers(), true));
        $this->assertTrue(\in_array('flow-azure-blob', \stream_get_wrappers(), true));
    }
}
