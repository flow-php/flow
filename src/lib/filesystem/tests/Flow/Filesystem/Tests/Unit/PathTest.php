<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit;

use Flow\Filesystem\Partition;
use Flow\Filesystem\Partitions;
use Flow\Filesystem\Path\Option;
use Flow\Filesystem\Path\Option\ContentType;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function array_map;
use function file_exists;
use function Flow\Filesystem\DSL\partition;
use function Flow\Filesystem\DSL\partitions;
use function Flow\Filesystem\DSL\path;
use function Flow\Filesystem\DSL\path_real;
use function mkdir;

final class PathTest extends TestCase
{
    /**
     * @return \Generator<int, array{string, string}>
     */
    public static function directories(): Generator
    {
        yield ['/some_file.txt', '/'];
        yield ['/some/nested/file.csv', '/some/nested'];
        yield ['flow-file://nested/file/path/file.txt', '/nested/file/path'];
    }

    /**
     * @return \Generator<int, array{string, string, string}>
     */
    public static function paths(): Generator
    {
        yield ['/file.csv', 'file', 'file://file.csv'];
        yield ['file://file.csv', 'file', 'file://file.csv'];
        yield ['file:///', 'file', 'file://'];
        yield ['/', 'file', 'file://'];
        yield ['/absolute/path/to/file.txt', 'file', 'file://absolute/path/to/file.txt'];
        yield ['file://absolute/path/to/file.txt', 'file', 'file://absolute/path/to/file.txt'];
        yield ['file:///absolute/path/to/file.txt', 'file', 'file://absolute/path/to/file.txt'];
        yield ['flow-file://', 'flow-file', 'flow-file://'];
        yield ['flow-file:///', 'flow-file', 'flow-file://'];
        yield ['flow-file://folder/file.csv', 'flow-file', 'flow-file://folder/file.csv'];
    }

    /**
     * @return \Generator<int, array{string, string, bool}>
     */
    public static function paths_pattern_matching(): Generator
    {
        yield ['/file.csv', '/file.csv', true];
        yield ['/nested/folder/any/file.csv', '/nested/folder/*/file.csv', false];
        yield ['/nested/folder/*/file.csv', '/nested/folder/any/file.csv', true];
        yield ['/nested/folder/[a]*/file.csv', '/nested/folder/ab/file.csv', true];
        yield ['/nested/folder/**/file.csv', '/nested/folder/any/nested/file.csv', true];
        yield ['/nested/folder/**/fil?.csv', '/nested/folder/any/nested/file.csv', true];
    }

    /**
     * @return \Generator<int, array{string, Partitions}>
     */
    public static function paths_with_partitions(): Generator
    {
        yield ['/', partitions()];
        yield ['file://path/without/partitions/file.csv', partitions()];
        yield ['file://path/country=US/file.csv', partitions(partition('country', 'US'))];
        yield [
            'file://path/country=US/region=america/file.csv',
            partitions(partition('country', 'US'), partition('region', 'america')),
        ];
        yield ['file://path/country=*/file.csv', partitions()];
    }

    /**
     * @return \Generator<int, array{string, string}>
     */
    public static function paths_with_static_parts(): Generator
    {
        yield ['/file.csv', '/file.csv'];
        yield ['/nested/folder', '/nested/folder/*/file.csv'];
        yield ['/nested/folder/path', '/nested/folder/path/{one|two}/file.csv'];
        yield ['/', '/file*.csv'];
        yield ['/', '/{one|two|tree}.csv'];
        yield ['/', '/file.{parquet|csv}'];
        yield ['flow-file://nested', 'flow-file://nested/partition={one,two}/*.csv'];
        yield ['flow-file://nested', 'flow-file://nested/partition=[one]/*.csv'];
        yield ['file://nested', '/nested/partition=[one]/*.csv'];
    }

    protected function setUp(): void
    {
        if (!file_exists(__DIR__ . '/var')) {
            mkdir(__DIR__ . '/var');
        }
    }

    public function test_add_partitions_to_path_pattern(): void
    {
        $this->expectExceptionMessage("Can't add partitions to path pattern.");

        path('/path/to/group=*/file.txt')->addPartitions(partition('group', 'a'));
    }

    public function test_add_partitions_to_path_with_extension(): void
    {
        static::assertEquals(
            path('/path/to/group=a/file.txt'),
            path('/path/to/file.txt')->addPartitions(partition('group', 'a')),
        );
    }

    public function test_add_partitions_to_path_without_extension(): void
    {
        static::assertEquals(
            path('/path/to/group=a/folder'),
            path('/path/to/folder')->addPartitions(partition('group', 'a')),
        );
    }

    public function test_add_partitions_to_root_path_with_extension(): void
    {
        static::assertEquals(path('/group=a/file.txt'), path('/file.txt')->addPartitions(partition('group', 'a')));
    }

    public function test_add_partitions_to_root_path_without_extension(): void
    {
        static::assertEquals(path('/group=a/folder'), path('/folder')->addPartitions(partition('group', 'a')));
    }

    #[DataProvider('directories')]
    public function test_directories(string $uri, string $dirPath): void
    {
        static::assertSame($dirPath, path($uri)->parentDirectory()->path());
    }

    public function test_extension(): void
    {
        static::assertSame('php', path(__FILE__)->extension());
        static::assertFalse(path(__DIR__)->extension());
    }

    public function test_extension_uppercase(): void
    {
        static::assertSame('php', path('/var/file/code.PhP')->extension());
    }

    public function test_extract_placeholder_partitions(): void
    {
        $pattern = path('/output/order-year=2024/{order-name}.csv');

        static::assertEquals(
            partitions(partition('order-name', '123456-PL')),
            $pattern->extractPlaceholderPartitions(path('/output/order-year=2024/123456-PL.csv')),
        );
    }

    public function test_file_prefix(): void
    {
        $path = path('flow-file://var/dir/file.csv', []);

        static::assertSame('flow-file://var/dir/._flow_tmp.file.csv', $path->basenamePrefix('._flow_tmp.')->uri());
        static::assertSame('csv', $path->extension());
    }

    public function test_file_prefix_on_directory(): void
    {
        $path = path('flow-file://var/dir/', []);

        static::assertSame('flow-file://var/._flow_tmp.dir', $path->basenamePrefix('._flow_tmp.')->uri());
        static::assertFalse($path->extension());
    }

    public function test_file_prefix_on_root_directory(): void
    {
        $path = path('flow-file://', []);

        static::assertSame('flow-file://._flow_tmp.', $path->basenamePrefix('._flow_tmp.')->uri());
        static::assertFalse($path->extension());
    }

    #[DataProvider('paths_with_static_parts')]
    public function test_finding_static_part_of_the_path(string $staticPart, string $uri): void
    {
        static::assertEquals(path($staticPart), path($uri)->staticPart());
    }

    public function test_glob(): void
    {
        static::assertSame('/output/*.csv', path('/output/{order-name}.csv')->glob());
        static::assertSame('/output/file.csv', path('/output/file.csv')->glob());
    }

    public function test_local_file(): void
    {
        static::assertNull(path(__FILE__)->context()->resource());
    }

    #[DataProvider('paths_pattern_matching')]
    public function test_matching_pattern_with_path(string $path, string $pattern, bool $result): void
    {
        static::assertSame($result, path($path)->matches(path($pattern)));
    }

    public function test_not_matching_items_under_directory_that_matches_pattern(): void
    {
        $path = path('flow-file://var/file/partition=*');

        static::assertTrue($path->matches(path('flow-file://var/file/partition=1')));
        static::assertFalse($path->matches(path('flow-file://var/file/partition=1/file.csv')));
    }

    #[DataProvider('paths')]
    public function test_parsing_path(string $uri, string $schema, string $parsedUri): void
    {
        static::assertEquals($schema, path($uri)->protocol());
        static::assertEquals($parsedUri, path($uri)->uri());
    }

    public function test_partition_placeholders(): void
    {
        static::assertEquals(['order-name'], path('/output/{order-name}.csv')->partitionPlaceholders());
        static::assertEquals([], path('/output/file.csv')->partitionPlaceholders());
    }

    #[DataProvider('paths_with_partitions')]
    public function test_partitions_in_path(string $uri, Partitions $partitions): void
    {
        static::assertEquals($partitions, path($uri)->partitions());
    }

    public function test_partitions_paths(): void
    {
        $path = path('/var/path/partition_1=A/partition_2=B/file.csv', ['option' => true]);

        static::assertEquals(
            [
                path('/var/path/partition_1=A', ['option' => true]),
                path('/var/path/partition_1=A/partition_2=B', ['option' => true]),
            ],
            $path->partitionsPaths(),
        );
    }

    public function test_randomization_file_path(): void
    {
        $path = path('flow-file://var/file/test.csv', []);

        static::assertStringStartsWith('flow-file://var/file/test_', $path->randomize()->uri());
        static::assertStringEndsWith('.csv', $path->randomize()->uri());
    }

    public function test_randomization_folder_path(): void
    {
        $path = path('flow-file://var/file/folder/', []);

        static::assertStringStartsWith('flow-file://var/file/folder_', $path->randomize()->uri());
    }

    public function test_real_path_on_custom_schema(): void
    {
        $path = path_real('azure-blob://var/dir/file.php');

        static::assertSame('azure-blob://var/dir/file.php', $path->uri());
    }

    #[TestWith(['file://var/www/index.html', 'var'])]
    #[TestWith(['file://index.html', null])]
    #[TestWith(['file://index', null])]
    #[TestWith(['file://var/www', 'var'])]
    #[TestWith(['/var/www', 'var'])]
    #[TestWith(['var/www', 'var'])]
    #[TestWith(['www', null])]
    public function test_root_directory_name(string $path, ?string $rootDirectory): void
    {
        static::assertEquals($rootDirectory, path($path)->rootDirectoryName());
    }

    public function test_set_extension(): void
    {
        $path = path('flow-file://var/dir/file.csv', []);

        static::assertSame('flow-file://var/dir/file.parquet', $path->setExtension('parquet')->uri());
    }

    public function test_set_extension_on_directory(): void
    {
        $path = path('flow-file://var/dir/', []);

        static::assertSame('flow-file://var/dir.parquet', $path->setExtension('parquet')->uri());
    }

    public function test_set_extension_on_file_without_extension(): void
    {
        $path = path('flow-file://var/dir/file', []);

        static::assertSame('flow-file://var/dir/file.parquet', $path->setExtension('parquet')->uri());
    }

    public function test_set_option(): void
    {
        $path = path('flow-file://var/dir/file.csv')->setOption(Option::CONTENT_TYPE, ContentType::TEXT);

        static::assertEquals(ContentType::TEXT, $path->getOption(Option::CONTENT_TYPE));
    }

    public function test_set_option_when_empty(): void
    {
        $path = path('flow-file://var/dir/file.csv')
            ->setOptionWhenEmpty(Option::CONTENT_TYPE, ContentType::TEXT)
            ->setOptionWhenEmpty(Option::CONTENT_TYPE, ContentType::CSV);

        static::assertEquals(ContentType::TEXT, $path->getOption(Option::CONTENT_TYPE));
        static::assertTrue($path->hasOption(Option::CONTENT_TYPE));
        static::assertFalse($path->hasOption('test'));
    }

    #[TestWith(['file://var/www/index.html', 1, 'file://www/index.html'])]
    #[TestWith(['file://var/www/index.html', 2, 'file://index.html'])]
    #[TestWith(['file://var/www/index.html', 3, null])]
    #[TestWith(['file://index.html', 1, null])]
    public function test_skip_directories(string $path, int $count, ?string $newPath): void
    {
        if ($newPath === null) {
            static::assertNull(path($path)->skipDirectories($count));
        } else {
            static::assertEquals(path($newPath), path($path)->skipDirectories($count));
        }
    }

    public function test_suffix(): void
    {
        $path = path('flow-file://var/dir', []);

        static::assertSame('flow-file://var/dir/test.csv', $path->suffix('test.csv')->uri());

        static::assertSame('flow-file://var/dir/test.csv', $path->suffix('/test.csv')->uri());
    }

    public function test_with_partitions(): void
    {
        $path = path('/output/order-year=2024/123456-PL.csv')->withPartitions(partitions(partition(
            'order-name',
            '123456-PL',
        )));

        static::assertEquals(
            partitions(partition('order-year', '2024'), partition('order-name', '123456-PL')),
            $path->partitions(),
        );
    }

    public function test_add_partitions_keeps_partitions_attached_with_with_partitions(): void
    {
        // B16: addPartitions() used to construct a fresh Path without the explicitly attached ones
        $path = path('/data/file.csv')
            ->withPartitions(new Partitions(new Partition('region', 'eu')))
            ->addPartitions(new Partition('year', '2024'));

        static::assertSame(
            ['year', 'region'],
            array_map(static fn(Partition $p): string => $p->name, $path->partitions()->toArray()),
        );
    }
}
