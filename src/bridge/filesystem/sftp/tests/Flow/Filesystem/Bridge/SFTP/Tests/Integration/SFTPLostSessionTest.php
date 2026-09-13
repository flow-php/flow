<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\Tests\Integration;

use Flow\Filesystem\Bridge\SFTP\Tests\Context\SFTPContext;
use Flow\Filesystem\Exception\RuntimeException;

use function Flow\Filesystem\Bridge\SFTP\DSL\sftp_filesystem;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class SFTPLostSessionTest extends SFTPTestCase
{
    public function test_listing_fails_instead_of_reporting_an_empty_directory(): void
    {
        $this->sftpContext()->givenFileExists(path('sftp:///upload/orders.csv'), 'a');

        $filesystem = sftp_filesystem($sftp = SFTPContext::connect());
        $sftp->disconnect();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('SFTP session is no longer usable');

        iterator_to_array($filesystem->list(path('sftp:///upload')), false);
    }

    public function test_moving_fails_instead_of_reporting_a_missing_source(): void
    {
        $this->sftpContext()->givenFileExists(path('sftp:///upload/orders.csv'), 'a');

        $filesystem = sftp_filesystem($sftp = SFTPContext::connect());
        $sftp->disconnect();

        $this->expectException(RuntimeException::class);

        $filesystem->mv(path('sftp:///upload/orders.csv'), path('sftp:///upload/moved.csv'));
    }

    public function test_reading_fails_instead_of_returning_empty_content(): void
    {
        $this->sftpContext()->givenFileExists(path('sftp:///upload/orders.csv'), "id,name\n1,one\n");

        $filesystem = sftp_filesystem($sftp = SFTPContext::connect());

        static::assertSame("id,name\n1,one\n", $filesystem->readFrom(path('sftp:///upload/orders.csv'))->content());

        $sftp->disconnect();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('SFTP session is no longer usable');

        $filesystem->readFrom(path('sftp:///upload/orders.csv'))->content();
    }

    public function test_removing_fails_instead_of_reporting_nothing_removed(): void
    {
        $this->sftpContext()->givenFileExists(path('sftp:///upload/orders.csv'), 'a');

        $filesystem = sftp_filesystem($sftp = SFTPContext::connect());
        $sftp->disconnect();

        $this->expectException(RuntimeException::class);

        $filesystem->rm(path('sftp:///upload/orders.csv'));
    }

    public function test_size_fails_instead_of_reporting_null(): void
    {
        $this->sftpContext()->givenFileExists(path('sftp:///upload/orders.csv'), '0123456789');

        $filesystem = sftp_filesystem($sftp = SFTPContext::connect());
        $sftp->disconnect();

        $this->expectException(RuntimeException::class);

        $filesystem->readFrom(path('sftp:///upload/orders.csv'))->size();
    }

    public function test_status_fails_instead_of_reporting_a_missing_file(): void
    {
        $this->sftpContext()->givenFileExists(path('sftp:///upload/orders.csv'), '0123456789');

        $filesystem = sftp_filesystem($sftp = SFTPContext::connect());
        $sftp->disconnect();

        $this->expectException(RuntimeException::class);

        $filesystem->status(path('sftp:///upload/orders.csv'));
    }

    public function test_uploading_a_block_fails_instead_of_writing_nothing(): void
    {
        $filesystem = sftp_filesystem($sftp = SFTPContext::connect());
        $stream = $filesystem->writeTo(path('sftp:///upload/orders.csv'));
        $stream->append("id\n1\n");

        $sftp->disconnect();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('SFTP session is no longer usable');

        $stream->close();
    }
}
