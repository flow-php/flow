<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\Tests\Unit;

use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Bridge\SFTP\DirectoryTraversal\RemoteEntries;
use Flow\Filesystem\Bridge\SFTP\DirectoryTraversal\RemoteEntryType;
use Flow\Filesystem\Bridge\SFTP\Tests\Double\FixedListingSFTP;

use function iterator_to_array;

final class RemoteEntriesTest extends FlowTestCase
{
    public function test_a_listing_the_server_refused_is_reported_as_null(): void
    {
        $sftp = new FixedListingSFTP(false);

        static::assertNull(RemoteEntries::in($sftp, '/upload'));
    }

    public function test_current_and_parent_directory_are_skipped(): void
    {
        $sftp = new FixedListingSFTP([
            '.' => ['filename' => '.', 'type' => 2],
            '..' => ['filename' => '..', 'type' => 2],
            'orders.csv' => ['filename' => 'orders.csv', 'type' => 1],
        ]);

        $entries = iterator_to_array(RemoteEntries::in($sftp, '/upload') ?? [], false);

        static::assertCount(1, $entries);
        static::assertSame('orders.csv', $entries[0]->name);
    }

    public function test_directory_entry(): void
    {
        $sftp = new FixedListingSFTP([
            '2024' => ['filename' => '2024', 'type' => 2, 'size' => 4096, 'mtime' => 1],
        ]);

        $entries = iterator_to_array(RemoteEntries::in($sftp, '/upload') ?? [], false);

        static::assertSame(RemoteEntryType::DIRECTORY, $entries[0]->type);
        static::assertTrue($entries[0]->isDirectory());
    }

    public function test_entries_are_sorted_by_name(): void
    {
        $sftp = new FixedListingSFTP([
            'c.csv' => ['filename' => 'c.csv', 'type' => 1],
            'a.csv' => ['filename' => 'a.csv', 'type' => 1],
            'b.csv' => ['filename' => 'b.csv', 'type' => 1],
        ]);

        $entries = iterator_to_array(RemoteEntries::in($sftp, '/upload') ?? [], false);

        static::assertSame(['a.csv', 'b.csv', 'c.csv'], [
            $entries[0]->name,
            $entries[1]->name,
            $entries[2]->name,
        ]);
    }

    public function test_missing_attributes_fall_back_to_the_listing_key(): void
    {
        $sftp = new FixedListingSFTP(['orders.csv' => []]);

        $entries = iterator_to_array(RemoteEntries::in($sftp, '/upload') ?? [], false);

        static::assertSame('orders.csv', $entries[0]->name);
        static::assertSame(RemoteEntryType::UNKNOWN, $entries[0]->type);
        static::assertNull($entries[0]->size);
        static::assertNull($entries[0]->lastModifiedAt);
    }

    public function test_modification_time_is_read_from_the_unix_timestamp(): void
    {
        $sftp = new FixedListingSFTP([
            'orders.csv' => ['filename' => 'orders.csv', 'type' => 1, 'mtime' => 1_700_000_000],
        ]);

        $entries = iterator_to_array(RemoteEntries::in($sftp, '/upload') ?? [], false);

        static::assertSame(1_700_000_000, $entries[0]->lastModifiedAt?->getTimestamp());
    }

    public function test_regular_file_entry(): void
    {
        $sftp = new FixedListingSFTP([
            'orders.csv' => ['filename' => 'orders.csv', 'type' => 1, 'size' => 128, 'mtime' => 1_700_000_000],
        ]);

        $entries = iterator_to_array(RemoteEntries::in($sftp, '/upload') ?? [], false);

        static::assertSame('orders.csv', $entries[0]->name);
        static::assertSame(128, $entries[0]->size);
        static::assertFalse($entries[0]->isDirectory());
    }

    public function test_size_reported_as_a_float_for_a_huge_file_is_narrowed_to_int(): void
    {
        $sftp = new FixedListingSFTP([
            'big.bin' => ['filename' => 'big.bin', 'type' => 1, 'size' => 1024.0],
        ]);

        $entries = iterator_to_array(RemoteEntries::in($sftp, '/upload') ?? [], false);

        static::assertSame(1024, $entries[0]->size);
    }

    public function test_symlink_is_not_reported_as_a_directory(): void
    {
        $sftp = new FixedListingSFTP(['link' => ['filename' => 'link', 'type' => 3]]);

        $entries = iterator_to_array(RemoteEntries::in($sftp, '/upload') ?? [], false);

        static::assertSame(RemoteEntryType::SYMLINK, $entries[0]->type);
        static::assertFalse($entries[0]->isDirectory());
    }

    public function test_unknown_numeric_type_does_not_blow_up(): void
    {
        $sftp = new FixedListingSFTP(['weird' => ['filename' => 'weird', 'type' => 99]]);

        $entries = iterator_to_array(RemoteEntries::in($sftp, '/upload') ?? [], false);

        static::assertSame(RemoteEntryType::UNKNOWN, $entries[0]->type);
    }
}
