<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\IO\Floe\FloeExtractor;
use Flow\ETL\Storage\Floe\FloeFiles;
use Flow\ETL\Storage\Floe\FloeStreamWriter;
use Flow\ETL\Storage\Floe\FooterReader;
use Flow\ETL\Storage\Floe\Options;
use Flow\ETL\Values\Column\AdaptiveBackend;
use Flow\ETL\Values\Column\Frame\Codec;
use Flow\ETL\Values\Column\Frame\NoopCodec;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;

use function array_map;
use function explode;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\path;
use function Flow\Floe\DSL\from_floe;
use function range;

final class FloeFilesContext
{
    /**
     * The columns the file body carries, read from its footer: a partition column taken from the path is not one.
     *
     * @return list<string>
     */
    public static function columns(Filesystem $filesystem, Path $file): array
    {
        return Schema::fromArray(
            (new FooterReader())->read($filesystem->readFrom($file), new NoopCodec())->footer->schema,
        )->names();
    }

    /**
     * $rows written to $uri, and the extractor that reads them back.
     */
    public static function extractor(Filesystem $filesystem, string $uri, Rows $rows): FloeExtractor
    {
        self::writeFiles($filesystem, [$uri => $rows]);

        return from_floe(path($uri), filesystem: $filesystem);
    }

    public static function files(Filesystem $filesystem, Codec $codec = new NoopCodec()): FloeFiles
    {
        return new FloeFiles($filesystem, new AdaptiveBackend(), new Options(codec: $codec));
    }

    /**
     * A two-file Hive tree whose bodies carry only `id`, so `country` can only come from the path.
     */
    public static function writePartitionedFiles(Filesystem $filesystem, string $root = 'memory://parts'): void
    {
        $schema = schema(int_schema('id'));

        foreach (['PL' => 1, 'DE' => 2] as $country => $id) {
            self::writeAll(
                self::files($filesystem)->create(path($root . '/country=' . $country . '/data.floe'), $schema),
                [array_to_rows([['id' => $id]], $schema)],
            );
        }
    }

    /**
     * The same tree with a numerically-valued partition, so a declared partition type has something
     * to convert - `country=PL` is already a string and would prove nothing.
     */
    public static function writeYearPartitionedFiles(Filesystem $filesystem, string $root = 'memory://years'): void
    {
        $schema = schema(int_schema('id'));

        foreach (['2024' => 1, '2025' => 2] as $year => $id) {
            self::writeAll(
                self::files($filesystem)->create(path($root . '/year=' . $year . '/data.floe'), $schema),
                [array_to_rows([['id' => $id]], $schema)],
            );
        }
    }

    /**
     * Seven daily files, 2022-12-30 to 2023-01-05, under year=/month=/day= directories: each body holds its date in
     * `text`, so year, month and day can only come from the path.
     */
    public static function writeDailyPartitionedFiles(Filesystem $filesystem, string $root): void
    {
        $schema = schema(str_schema('text'));

        foreach ([
            '2022-12-30',
            '2022-12-31',
            '2023-01-01',
            '2023-01-02',
            '2023-01-03',
            '2023-01-04',
            '2023-01-05',
        ] as $day) {
            [$year, $month, $dayOfMonth] = explode('-', $day);
            self::writeFiles($filesystem, [
                $root
                    . '/year='
                    . $year
                    . '/month='
                    . (int) $month
                    . '/day='
                    . (int) $dayOfMonth
                    . '/file.floe' => array_to_rows([['text' => $day]], $schema),
            ]);
        }
    }

    /**
     * @param array<string, Rows> $files - uri => the rows written to it
     */
    public static function writeFiles(Filesystem $filesystem, array $files): void
    {
        foreach ($files as $uri => $rows) {
            self::writeAll(self::files($filesystem)->create(path($uri), $rows->schema()), [$rows]);
        }
    }

    /**
     * One section per Rows, in order: the first creates the file, every later one is appended and closes its own
     * section, so a byte window can cut between them.
     */
    public static function writeSectioned(Filesystem $filesystem, string $uri, Rows ...$sections): void
    {
        foreach ($sections as $position => $section) {
            self::writeAll(
                $position === 0
                    ? self::files($filesystem)->create(path($uri), $section->schema())
                    : self::files($filesystem)->append(path($uri), $section->schema()),
                [$section],
            );
        }
    }

    /**
     * Ids 1..($sections * $rowsPerSection) in $sections sections of $rowsPerSection rows: every append closes a section.
     */
    public static function writeSections(Filesystem $filesystem, string $uri, int $sections, int $rowsPerSection): void
    {
        $schema = schema(int_schema('id'));

        for ($section = 0; $section < $sections; $section++) {
            $writer = $section === 0
                ? self::files($filesystem)->create(path($uri), $schema)
                : self::files($filesystem)->append(path($uri), $schema);
            $writer->write(array_to_rows(
                array_map(
                    static fn(int $id): array => ['id' => $id],
                    range(($section * $rowsPerSection) + 1, ($section + 1) * $rowsPerSection),
                ),
                $schema,
            ));
            $writer->close();
        }
    }

    /**
     * @param array<int, Rows> $batches
     */
    public static function writeAll(FloeStreamWriter $writer, array $batches): void
    {
        foreach ($batches as $batch) {
            $writer->write($batch);
        }

        $writer->close();
    }
}
