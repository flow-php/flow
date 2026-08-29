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
        static::assertNull(RemoteEntries::in(new FixedListingSFTP(false), '/upload'));
    }

    public function test_current_and_parent_directory_are_skipped(): void
    {
        $listing = RemoteEntries::in(new FixedListingSFTP([
            '.' => ['filename' => '.', 'type' => 2],
            '..' => ['filename' => '..', 'type' => 2],
            'orders.csv' => ['filename' => 'orders.csv', 'type' => 1],
        ]), '/upload');

        static::assertNotNull($listing);

        $entries = iterator_to_array($listing, false);

        static::assertCount(1, $entries);
        static::assertSame('orders.csv', $entries[0]->name);
    }

    public function test_directory_entry(): void
    {
        $listing = RemoteEntries::in(new FixedListingSFTP([
            '2024' => ['filename' => '2024', 'type' => 2, 'size' => 4096, 'mtime' => 1],
        ]), '/upload');

        static::assertNotNull($listing);

        $entries = iterator_to_array($listing, false);

        static::assertTrue($entries[0]->isDirectory());
        static::assertSame(RemoteEntryType::DIRECTORY, $entries[0]->type);
    }

    public function test_entries_are_sorted_by_name(): void
    {
        $listing = RemoteEntries::in(new FixedListingSFTP([
            'c.csv' => ['filename' => 'c.csv', 'type' => 1],
            'a.csv' => ['filename' => 'a.csv', 'type' => 1],
            'b.csv' => ['filename' => 'b.csv', 'type' => 1],
        ]), '/upload');

        static::assertNotNull($listing);

        $entries = iterator_to_array($listing, false);

        static::assertSame(['a.csv', 'b.csv', 'c.csv'], [
            $entries[0]->name,
            $entries[1]->name,
            $entries[2]->name,
        ]);
    }

    public function test_missing_attributes_fall_back_to_the_listing_key(): void
    {
        $listing = RemoteEntries::in(new FixedListingSFTP(['orders.csv' => []]), '/upload');

        static::assertNotNull($listing);

        $entries = iterator_to_array($listing, false);

        static::assertSame('orders.csv', $entries[0]->name);
        static::assertSame(RemoteEntryType::UNKNOWN, $entries[0]->type);
        static::assertNull($entries[0]->size);
        static::assertNull($entries[0]->lastModifiedAt);
    }

    public function test_modification_time_is_read_from_the_unix_timestamp(): void
    {
        $listing = RemoteEntries::in(new FixedListingSFTP([
            'orders.csv' => ['filename' => 'orders.csv', 'type' => 1, 'mtime' => 1_700_000_000],
        ]), '/upload');

        static::assertNotNull($listing);

        $entries = iterator_to_array($listing, false);

        static::assertSame(1_700_000_000, $entries[0]->lastModifiedAt?->getTimestamp());
    }

    public function test_regular_file_entry(): void
    {
        $listing = RemoteEntries::in(new FixedListingSFTP([
            'orders.csv' => ['filename' => 'orders.csv', 'type' => 1, 'size' => 128, 'mtime' => 1_700_000_000],
        ]), '/upload');

        static::assertNotNull($listing);

        $entries = iterator_to_array($listing, false);

        static::assertSame('orders.csv', $entries[0]->name);
        static::assertFalse($entries[0]->isDirectory());
        static::assertSame(128, $entries[0]->size);
    }

    public function test_size_reported_as_a_float_for_a_huge_file_is_narrowed_to_int(): void
    {
        $listing = RemoteEntries::in(new FixedListingSFTP([
            'big.bin' => ['filename' => 'big.bin', 'type' => 1, 'size' => 1024.0],
        ]), '/upload');

        static::assertNotNull($listing);

        $entries = iterator_to_array($listing, false);

        static::assertSame(1024, $entries[0]->size);
    }

    public function test_symlink_is_not_reported_as_a_directory(): void
    {
        $listing = RemoteEntries::in(new FixedListingSFTP([
            'link' => ['filename' => 'link', 'type' => 3],
        ]), '/upload');

        static::assertNotNull($listing);

        $entries = iterator_to_array($listing, false);

        static::assertSame(RemoteEntryType::SYMLINK, $entries[0]->type);
        static::assertFalse($entries[0]->isDirectory());
    }

    public function test_unknown_numeric_type_does_not_blow_up(): void
    {
        $listing = RemoteEntries::in(new FixedListingSFTP([
            'weird' => ['filename' => 'weird', 'type' => 99],
        ]), '/upload');

        static::assertNotNull($listing);

        $entries = iterator_to_array($listing, false);

        static::assertSame(RemoteEntryType::UNKNOWN, $entries[0]->type);
    }
}
