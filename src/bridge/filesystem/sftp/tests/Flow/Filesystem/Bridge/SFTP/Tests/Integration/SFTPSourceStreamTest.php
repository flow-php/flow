<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\Tests\Integration;

use function Flow\Filesystem\Bridge\SFTP\DSL\sftp_filesystem_options;
use function Flow\Filesystem\DSL\path;
use function implode;
use function iterator_to_array;
use function str_repeat;
use function strlen;

final class SFTPSourceStreamTest extends SFTPTestCase
{
    public function test_iterating_a_file_in_fixed_size_chunks(): void
    {
        $this->sftpContext()->givenFileExists(path('sftp:///upload/orders.csv'), '0123456789');

        $chunks = iterator_to_array(
            $this->sftpContext()->filesystem()->readFrom(path('sftp:///upload/orders.csv'))->iterate(4),
            false,
        );

        static::assertSame(['0123', '4567', '89'], $chunks);
    }

    public function test_reading_a_range_from_the_middle_of_a_file(): void
    {
        $this->sftpContext()->givenFileExists(path('sftp:///upload/orders.csv'), '0123456789');

        static::assertSame('345', $this
            ->sftpContext()
            ->filesystem()
            ->readFrom(path('sftp:///upload/orders.csv'))
            ->read(3, 3));
    }

    public function test_reading_lines_across_chunk_boundaries(): void
    {
        $lines = [];

        for ($i = 1; $i <= 500; $i++) {
            $lines[] = 'line-' . $i;
        }

        $this->sftpContext()->givenFileExists(path('sftp:///upload/lines.txt'), implode("\n", $lines));

        $options = sftp_filesystem_options()->withReadChunkSize(64);

        static::assertSame($lines, iterator_to_array(
            $this->sftpContext()->filesystem($options)->readFrom(path('sftp:///upload/lines.txt'))->readLines(),
            false,
        ));
    }

    public function test_reading_the_whole_content(): void
    {
        $this->sftpContext()->givenFileExists(path('sftp:///upload/orders.csv'), $content = "id,name\n1,one\n2,two\n");

        static::assertSame(
            $content,
            $this->sftpContext()->filesystem()->readFrom(path('sftp:///upload/orders.csv'))->content(),
        );
    }

    public function test_reading_with_a_negative_offset_reads_from_the_end(): void
    {
        $this->sftpContext()->givenFileExists(path('sftp:///upload/orders.csv'), '0123456789');

        static::assertSame('89', $this
            ->sftpContext()
            ->filesystem()
            ->readFrom(path('sftp:///upload/orders.csv'))
            ->read(2, -2));
    }

    public function test_size_of_a_file_larger_than_a_single_read(): void
    {
        $this->sftpContext()->givenFileExists(path('sftp:///upload/big.txt'), $content = str_repeat('a', 100_000));

        static::assertSame(
            strlen($content),
            $this->sftpContext()->filesystem()->readFrom(path('sftp:///upload/big.txt'))->size(),
        );
    }
}
