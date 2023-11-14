<?php declare(strict_types=1);

namespace Flow\ParquetViewer\Tests\Integration;

use Flow\ParquetViewer\Parquet;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Tester\ApplicationTester;

final class ReadMetadataTest extends TestCase
{
    public function test_reading_metadata_from_non_json_file() : void
    {
        $application = new Parquet();
        $application->setAutoExit(false);
        $application->setCatchExceptions(false);

        $path = \realpath(__DIR__ . '/../../Fixtures/flow.json');

        $tester = new ApplicationTester($application);
        $tester->run([
            'command' => 'read:metadata',
            'file' => $path,
        ]);

        $this->assertStringContainsString(
            'not a valid parquet file',
            $tester->getDisplay()
        );
        $this->assertSame(1, $tester->getStatusCode());
    }

    public function test_reading_metadata_from_parquet_file() : void
    {
        $application = new Parquet();
        $application->setAutoExit(false);
        $application->setCatchExceptions(false);

        $path = \realpath(__DIR__ . '/../../Fixtures/flow.parquet');

        $tester = new ApplicationTester($application);
        $tester->run([
            'command' => 'read:metadata',
            'file' => $path,
            '--row-groups' => 1,
            '--page-headers' => 1,
            '--column-chunks' => 1,
            '--statistics' => 1,
        ]);

        $this->assertStringContainsString(
            <<<'OUTPUT'
┌──────────┬───── Row Groups ┬───────────────┐
│ num rows │ total byte size │ columns count │
├──────────┼─────────────────┼───────────────┤
│ 100      │ 44,102          │ 16            │
└──────────┴────── Total: 1 ─┴───────────────┘

┌────────────────────────────────┬────────────────────────────────────── Column Chunks ┬─────────────┬────────────┬────────────────────────┬──────────────────┐
│ path                           │ encodings                             │ compression │ file offset │ num values │ dictionary page offset │ data page offset │
├────────────────────────────────┼───────────────────────────────────────┼─────────────┼─────────────┼────────────┼────────────────────────┼──────────────────┤
│ boolean                        │ [RLE,PLAIN]                           │ SNAPPY      │ 4           │ 100        │ -                      │ 4                │
│ int32                          │ [RLE,PLAIN]                           │ SNAPPY      │ 44          │ 100        │ -                      │ 44               │
│ int64                          │ [RLE,PLAIN]                           │ SNAPPY      │ 747         │ 100        │ -                      │ 747              │
│ float                          │ [PLAIN_DICTIONARY,RLE,RLE_DICTIONARY] │ SNAPPY      │ 1,579       │ 100        │ 1,579                  │ 1,599            │
│ double                         │ [RLE,PLAIN]                           │ SNAPPY      │ 1,629       │ 100        │ -                      │ 1,629            │
│ decimal                        │ [RLE,PLAIN]                           │ SNAPPY      │ 2,061       │ 100        │ -                      │ 2,061            │
│ string                         │ [RLE,PLAIN]                           │ SNAPPY      │ 2,493       │ 100        │ -                      │ 2,493            │
│ date                           │ [RLE,PLAIN]                           │ SNAPPY      │ 4,997       │ 100        │ -                      │ 4,997            │
│ datetime                       │ [RLE,PLAIN]                           │ SNAPPY      │ 5,691       │ 100        │ -                      │ 5,691            │
│ list_of_datetimes.list.element │ [RLE,PLAIN]                           │ SNAPPY      │ 6,523       │ 300        │ -                      │ 6,523            │
│ map_of_ints.a                  │ [RLE,PLAIN]                           │ SNAPPY      │ 8,833       │ 100        │ -                      │ 8,833            │
│ map_of_ints.b                  │ [RLE,PLAIN]                           │ SNAPPY      │ 9,548       │ 100        │ -                      │ 9,548            │
│ map_of_ints.c                  │ [RLE,PLAIN]                           │ SNAPPY      │ 10,259      │ 100        │ -                      │ 10,259           │
│ list_of_strings.list.element   │ [RLE,PLAIN]                           │ SNAPPY      │ 10,970      │ 691        │ -                      │ 10,970           │
│ struct_flat.id                 │ [RLE,PLAIN]                           │ SNAPPY      │ 24,402      │ 100        │ -                      │ 24,402           │
│ struct_flat.name               │ [RLE,PLAIN]                           │ SNAPPY      │ 24,916      │ 100        │ -                      │ 24,916           │
└────────────────────────────────┴───────────────────────────────────────┴ Total: 16 ──┴─────────────┴────────────┴────────────────────────┴──────────────────┘

┌────────────────────────────────┬──────────────────────────────┬───────────────── Column Chunks Statistics ───────────────────┬───────────────────────────────┬────────────┬────────────────┐
│ path                           │ min [deprecated]             │ max [deprecated]              │ min value                    │ max value                     │ null count │ distinct count │
├────────────────────────────────┼──────────────────────────────┼───────────────────────────────┼──────────────────────────────┼───────────────────────────────┼────────────┼────────────────┤
│ Row Group: 0                                                                                                                                                                               │
├────────────────────────────────┼──────────────────────────────┼───────────────────────────────┼──────────────────────────────┼───────────────────────────────┼────────────┼────────────────┤
│ boolean                        │                              │ 1                             │                              │ 1                             │ -          │ 2              │
│ int32                          │ 12041874                     │ 2138721799                    │ 12041874                     │ 2138721799                    │ -          │ 100            │
│ int64                          │ 80604967340828891            │ 9139108325942554382           │ 80604967340828891            │ 9139108325942554382           │ -          │ 100            │
│ float                          │ 10.25                        │ 10.25                         │ 10.25                        │ 10.25                         │ -          │ 1              │
│ double                         │ 0                            │ 515214592                     │ 0                            │ 515214592                     │ -          │ 100            │
│ decimal                        │ 0                            │ 634690304                     │ 0                            │ 634690304                     │ -          │ 100            │
│ string                         │ A aut aperiam distinctio...  │ Voluptatem quo dolores...     │ A aut aperiam distinctio...  │ Voluptatem quo dolores...     │ -          │ 100            │
│ date                           │ 1672790400000000             │ 1698624000000000              │ 1672790400000000             │ 1698624000000000              │ -          │ 85             │
│ datetime                       │ 1672586016000000             │ 1698912601000000              │ 1672586016000000             │ 1698912601000000              │ -          │ 100            │
│ list_of_datetimes.list.element │ 1672577279000000             │ 1699336548000000              │ 1672577279000000             │ 1699336548000000              │ -          │ 300            │
│ map_of_ints.a                  │ 15173348                     │ 2145864542                    │ 15173348                     │ 2145864542                    │ -          │ 100            │
│ map_of_ints.b                  │ 8459873                      │ 2085922688                    │ 8459873                      │ 2085922688                    │ -          │ 100            │
│ map_of_ints.c                  │ 369858                       │ 2097290187                    │ 369858                       │ 2097290187                    │ -          │ 100            │
│ list_of_strings.list.element   │ A nesciunt autem nesciunt... │ Voluptatum voluptas maxime... │ A nesciunt autem nesciunt... │ Voluptatum voluptas maxime... │ -          │ 691            │
│ struct_flat.id                 │ 1                            │ 100                           │ 1                            │ 100                           │ -          │ 100            │
│ struct_flat.name               │ name_00001                   │ name_00100                    │ name_00001                   │ name_00100                    │ -          │ 100            │
└────────────────────────────────┴──────────────────────────────┴──────────────────────── Total: 16 ───────────────────────────┴───────────────────────────────┴────────────┴────────────────┘

┌────────────────────────────────┬─────────────────┬──────────────── Page Headers ──────┬───────────────────┬───────────────────────┬─────────────────┐
│ path                           │ type            │ encoding         │ compressed size │ uncompressed size │ dictionary num values │ data num values │
├────────────────────────────────┼─────────────────┼──────────────────┼─────────────────┼───────────────────┼───────────────────────┼─────────────────┤
│ boolean                        │ DATA_PAGE       │ PLAIN            │ 22              │ 20                │ -                     │ 100             │
│ int32                          │ DATA_PAGE       │ PLAIN            │ 683             │ 807               │ -                     │ 100             │
│ int64                          │ DATA_PAGE       │ PLAIN            │ 812             │ 807               │ -                     │ 100             │
│ float                          │ DICTIONARY_PAGE │ PLAIN_DICTIONARY │ 6               │ 4                 │ 1                     │ -               │
│ double                         │ DATA_PAGE       │ PLAIN            │ 412             │ 407               │ -                     │ 100             │
│ decimal                        │ DATA_PAGE       │ PLAIN            │ 412             │ 407               │ -                     │ 100             │
│ string                         │ DATA_PAGE       │ PLAIN            │ 2,484           │ 4,227             │ -                     │ 100             │
│ date                           │ DATA_PAGE       │ PLAIN            │ 674             │ 807               │ -                     │ 100             │
│ datetime                       │ DATA_PAGE       │ PLAIN            │ 812             │ 807               │ -                     │ 100             │
│ list_of_datetimes.list.element │ DATA_PAGE       │ PLAIN            │ 2,290           │ 2,487             │ -                     │ 300             │
│ map_of_ints.a                  │ DATA_PAGE       │ PLAIN            │ 695             │ 807               │ -                     │ 100             │
│ map_of_ints.b                  │ DATA_PAGE       │ PLAIN            │ 691             │ 807               │ -                     │ 100             │
│ map_of_ints.c                  │ DATA_PAGE       │ PLAIN            │ 691             │ 807               │ -                     │ 100             │
│ list_of_strings.list.element   │ DATA_PAGE       │ PLAIN            │ 13,410          │ 28,345            │ -                     │ 691             │
│ struct_flat.id                 │ DATA_PAGE       │ PLAIN            │ 494             │ 807               │ -                     │ 100             │
│ struct_flat.name               │ DATA_PAGE       │ PLAIN            │ 521             │ 1,407             │ -                     │ 100             │
└────────────────────────────────┴─────────────────┴────────────────── Total: 16 ───────┴───────────────────┴───────────────────────┴─────────────────┘
OUTPUT,
            $tester->getDisplay()
        );
        $this->assertSame(0, $tester->getStatusCode());
    }
}
