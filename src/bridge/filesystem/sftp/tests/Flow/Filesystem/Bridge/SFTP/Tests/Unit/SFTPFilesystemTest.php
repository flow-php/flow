<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\Tests\Unit;

use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Exception\InvalidSchemeException;
use Flow\Filesystem\Exception\RuntimeException;
use phpseclib3\Net\SFTP;

use function Flow\Filesystem\Bridge\SFTP\DSL\sftp_filesystem;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class SFTPFilesystemTest extends FlowTestCase
{
    public function test_append_to_rejects_mismatched_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);
        $this->expectExceptionMessage(
            'Scheme "aws-s3://" is not supported by this protocol. Expected scheme is "sftp://"',
        );

        sftp_filesystem(new SFTP('localhost'))->appendTo(path('aws-s3://bucket/orders.csv'));
    }

    public function test_appending_to_the_tmp_dir_is_rejected(): void
    {
        $filesystem = sftp_filesystem(new SFTP('localhost'));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot write to system tmp directory');

        $filesystem->appendTo($filesystem->getSystemTmpDir());
    }

    public function test_list_rejects_mismatched_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);

        iterator_to_array(sftp_filesystem(new SFTP('localhost'))->list(path('file:///var/orders.csv')));
    }

    public function test_mount_protocol_can_be_customized(): void
    {
        static::assertSame('my-sftp', sftp_filesystem(new SFTP('localhost'), protocol: 'my-sftp')->mount()->protocol);
    }

    public function test_mv_rejects_mismatched_destination_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);

        sftp_filesystem(new SFTP('localhost'))->mv(path('sftp:///a.csv'), path('file:///var/b.csv'));
    }

    public function test_mv_rejects_mismatched_source_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);

        sftp_filesystem(new SFTP('localhost'))->mv(path('file:///var/a.csv'), path('sftp:///b.csv'));
    }

    public function test_read_from_rejects_mismatched_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);

        sftp_filesystem(new SFTP('localhost'))->readFrom(path('file:///var/orders.csv'));
    }

    public function test_removing_the_tmp_dir_is_refused(): void
    {
        $filesystem = sftp_filesystem(new SFTP('localhost'));

        static::assertFalse($filesystem->rm($filesystem->getSystemTmpDir()));
    }

    public function test_rm_rejects_mismatched_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);

        sftp_filesystem(new SFTP('localhost'))->rm(path('file:///var/orders.csv'));
    }

    public function test_status_rejects_mismatched_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);

        sftp_filesystem(new SFTP('localhost'))->status(path('file:///var/orders.csv'));
    }

    public function test_tmp_dir_is_reported_as_a_directory(): void
    {
        $filesystem = sftp_filesystem(new SFTP('localhost'));

        static::assertTrue($filesystem->status($filesystem->getSystemTmpDir())?->isDirectory());
    }

    public function test_write_to_rejects_mismatched_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);

        sftp_filesystem(new SFTP('localhost'))->writeTo(path('file:///var/orders.csv'));
    }

    public function test_writing_to_the_tmp_dir_is_rejected(): void
    {
        $filesystem = sftp_filesystem(new SFTP('localhost'));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot write to system tmp directory');

        $filesystem->writeTo($filesystem->getSystemTmpDir());
    }
}
