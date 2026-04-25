<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS\Tests\Integration;

use function Flow\Filesystem\Bridge\AsyncAWS\DSL\aws_s3_filesystem;
use function Flow\Filesystem\DSL\path;
use Flow\Filesystem\Bridge\AsyncAWS\Options;
use Flow\Filesystem\Bridge\AsyncAWS\Tests\Double\RecordingS3Client;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use Flow\Filesystem\Tests\Double\RejectingFilter;

final class AsyncAWSS3FilesystemFileFastPathTest extends AsyncAWSS3TestCase
{
    public function test_list_disabled_fast_path_falls_back_to_listing_for_single_file() : void
    {
        $client = $this->recordingClient();
        $fs = aws_s3_filesystem(
            $this->bucket(),
            $client,
            (new Options())->withFileFastPath(false),
        );

        $fs->writeTo(path('aws-s3://var/orders/orders.csv'))->append('a,b')->close();

        $client->resetCounters();

        $statuses = \iterator_to_array($fs->list(path('aws-s3://var/orders/orders.csv')));

        self::assertCount(1, $statuses);
        self::assertSame(0, $client->headObjectCount, 'HEAD must not be issued when fast path is disabled');
        self::assertSame(1, $client->listObjectsV2Count, 'listObjectsV2 must run when fast path is disabled');
    }

    public function test_list_filter_is_applied_on_single_file_fast_path() : void
    {
        $client = $this->recordingClient();
        $fs = aws_s3_filesystem($this->bucket(), $client);

        $fs->writeTo(path('aws-s3://var/orders/orders.csv'))->append('a,b')->close();

        $client->resetCounters();

        $statuses = \iterator_to_array(
            $fs->list(path('aws-s3://var/orders/orders.csv'), new RejectingFilter()),
        );

        self::assertCount(0, $statuses);
        self::assertSame(1, $client->headObjectCount);
        self::assertSame(0, $client->listObjectsV2Count);
    }

    public function test_list_folder_path_with_trailing_slash_skips_head() : void
    {
        $client = $this->recordingClient();
        $fs = aws_s3_filesystem($this->bucket(), $client);

        $fs->writeTo(path('aws-s3://var/orders/a.csv'))->append('a')->close();
        $fs->writeTo(path('aws-s3://var/orders/b.csv'))->append('b')->close();

        $client->resetCounters();

        $statuses = \iterator_to_array($fs->list(path('aws-s3://var/orders/')));

        self::assertCount(2, $statuses);
        self::assertSame(0, $client->headObjectCount, 'HEAD must be skipped for paths ending with /');
        self::assertSame(1, $client->listObjectsV2Count);
    }

    public function test_list_non_existing_single_file_path_falls_back_to_listing() : void
    {
        $client = $this->recordingClient();
        $fs = aws_s3_filesystem($this->bucket(), $client);

        $client->resetCounters();

        $statuses = \iterator_to_array($fs->list(path('aws-s3://var/missing/file.csv')));

        self::assertCount(0, $statuses);
        self::assertSame(1, $client->headObjectCount, 'HEAD is attempted on non-pattern, non-folder path');
        self::assertSame(1, $client->listObjectsV2Count, 'fallback listing runs after NoSuchKey');
    }

    public function test_list_pattern_skips_head() : void
    {
        $client = $this->recordingClient();
        $fs = aws_s3_filesystem($this->bucket(), $client);

        $fs->writeTo(path('aws-s3://var/orders/a.csv'))->append('a')->close();
        $fs->writeTo(path('aws-s3://var/orders/b.csv'))->append('b')->close();

        $client->resetCounters();

        $statuses = \iterator_to_array($fs->list(path('aws-s3://var/orders/*.csv')));

        self::assertCount(2, $statuses);
        self::assertSame(0, $client->headObjectCount, 'HEAD must not be issued for pattern paths');
        self::assertSame(1, $client->listObjectsV2Count);
    }

    public function test_list_root_path_skips_head() : void
    {
        $client = $this->recordingClient();
        $fs = aws_s3_filesystem($this->bucket(), $client);

        $fs->writeTo(path('aws-s3://root.txt'))->append('x')->close();

        $client->resetCounters();

        \iterator_to_array($fs->list(path('aws-s3:///')));

        self::assertSame(0, $client->headObjectCount);
        self::assertSame(1, $client->listObjectsV2Count);
    }

    public function test_list_single_file_does_not_yield_prefix_siblings() : void
    {
        $client = $this->recordingClient();
        $fs = aws_s3_filesystem($this->bucket(), $client);

        $fs->writeTo(path('aws-s3://var/orders/file.txt'))->append('a')->close();
        $fs->writeTo(path('aws-s3://var/orders/file.txt.bak'))->append('b')->close();

        $client->resetCounters();

        $statuses = \iterator_to_array($fs->list(path('aws-s3://var/orders/file.txt')));

        self::assertCount(1, $statuses);
        self::assertSame('aws-s3://var/orders/file.txt', $statuses[0]->path->uri());
        self::assertSame(1, $client->headObjectCount);
        self::assertSame(0, $client->listObjectsV2Count);
    }

    public function test_list_single_file_yields_via_head_only_when_fast_path_enabled() : void
    {
        $client = $this->recordingClient();
        $fs = aws_s3_filesystem($this->bucket(), $client);

        $fs->writeTo(path('aws-s3://var/orders/orders.csv'))->append('a,b,c')->close();

        $client->resetCounters();

        $statuses = \iterator_to_array($fs->list(path('aws-s3://var/orders/orders.csv'), new OnlyFiles()));

        self::assertCount(1, $statuses);
        self::assertSame('aws-s3://var/orders/orders.csv', $statuses[0]->path->uri());
        self::assertTrue($statuses[0]->isFile());
        self::assertSame(1, $client->headObjectCount, 'exactly one HEAD must be issued');
        self::assertSame(0, $client->listObjectsV2Count, 'listObjectsV2 must NOT be issued for single-file fast path');
    }

    private function recordingClient() : RecordingS3Client
    {
        $configuration = [
            'pathStyleEndpoint' => true,
            'endpoint' => $_ENV['S3_ENDPOINT'],
            'region' => $_ENV['S3_REGION'],
            'accessKeyId' => $_ENV['S3_ACCESS_KEY_ID'],
            'accessKeySecret' => $_ENV['S3_SECRET_ACCESS_KEY'],
        ];

        /** @phpstan-ignore-next-line */
        return new RecordingS3Client($configuration);
    }
}
