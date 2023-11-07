<?php declare(strict_types=1);

namespace Flow\ParquetViewer\Tests\Integration;

use Flow\ParquetViewer\Application;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Tester\ApplicationTester;

final class ReadMetadataTest extends TestCase
{
    public function test_reading_metadata_from_non_json_file() : void
    {
        $application = new Application();
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
        $application = new Application();
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
│ 100      │ 42,561          │ 16            │
└──────────┴────── Total: 1 ─┴───────────────┘

┌────────────────────────────────┬────────────────────────────────────── Column Chunks ─┬─────────────┬────────────┬────────────────────────┬──────────────────┐
│ path                           │ encodings                             │ compression  │ file offset │ num values │ dictionary page offset │ data page offset │
├────────────────────────────────┼───────────────────────────────────────┼──────────────┼─────────────┼────────────┼────────────────────────┼──────────────────┤
│ boolean                        │ [RLE,PLAIN]                           │ UNCOMPRESSED │ 4           │ 100        │ -                      │ 4                │
│ int32                          │ [RLE,PLAIN]                           │ UNCOMPRESSED │ 42          │ 100        │ -                      │ 42               │
│ int64                          │ [RLE,PLAIN]                           │ UNCOMPRESSED │ 469         │ 100        │ -                      │ 469              │
│ float                          │ [PLAIN_DICTIONARY,RLE,RLE_DICTIONARY] │ UNCOMPRESSED │ 1,296       │ 100        │ 1,296                  │ 1,314            │
│ double                         │ [RLE,PLAIN]                           │ UNCOMPRESSED │ 1,342       │ 100        │ -                      │ 1,342            │
│ decimal                        │ [RLE,PLAIN]                           │ UNCOMPRESSED │ 2,169       │ 100        │ -                      │ 2,169            │
│ string                         │ [RLE,PLAIN]                           │ UNCOMPRESSED │ 2,696       │ 100        │ -                      │ 2,696            │
│ date                           │ [RLE,PLAIN]                           │ UNCOMPRESSED │ 6,943       │ 100        │ -                      │ 6,943            │
│ datetime                       │ [RLE,PLAIN]                           │ UNCOMPRESSED │ 7,370       │ 100        │ -                      │ 7,370            │
│ time                           │ [PLAIN_DICTIONARY,RLE,RLE_DICTIONARY] │ UNCOMPRESSED │ 8,197       │ 100        │ 8,197                  │ 8,227            │
│ list_of_datetimes.list.element │ [RLE,PLAIN]                           │ UNCOMPRESSED │ 8,279       │ 300        │ -                      │ 8,279            │
│ map_of_ints.key_value.key      │ [PLAIN_DICTIONARY,RLE,RLE_DICTIONARY] │ UNCOMPRESSED │ 10,786      │ 300        │ 10,786                 │ 10,815           │
│ map_of_ints.key_value.value    │ [RLE,PLAIN]                           │ UNCOMPRESSED │ 11,037      │ 300        │ -                      │ 11,037           │
│ list_of_strings.list.element   │ [RLE,PLAIN]                           │ UNCOMPRESSED │ 12,344      │ 691        │ -                      │ 12,344           │
│ struct_flat.id                 │ [RLE,PLAIN]                           │ UNCOMPRESSED │ 40,711      │ 100        │ -                      │ 40,711           │
│ struct_flat.name               │ [RLE,PLAIN]                           │ UNCOMPRESSED │ 41,138      │ 100        │ -                      │ 41,138           │
└────────────────────────────────┴───────────────────────────────────────┴ Total: 16 ───┴─────────────┴────────────┴────────────────────────┴──────────────────┘

┌────────────────────────────────┬──────────────────────────────┬───────────────── Column Chunks Statistics ───────────────────┬───────────────────────────────┬────────────┬────────────────┐
│ path                           │ min [deprecated]             │ max [deprecated]              │ min value                    │ max value                     │ null count │ distinct count │
├────────────────────────────────┼──────────────────────────────┼───────────────────────────────┼──────────────────────────────┼───────────────────────────────┼────────────┼────────────────┤
│ Row Group: 0                                                                                                                                                                               │
├────────────────────────────────┼──────────────────────────────┼───────────────────────────────┼──────────────────────────────┼───────────────────────────────┼────────────┼────────────────┤
│ boolean                        │                              │ 1                             │                              │ 1                             │ -          │ 2              │
│ int32                          │ 12041874                     │ 2138721799                    │ 12041874                     │ 2138721799                    │ -          │ 100            │
│ int64                          │ 80604967340828891            │ 9139108325942554382           │ 80604967340828891            │ 9139108325942554382           │ -          │ 100            │
│ float                          │ 10.25                        │ 10.25                         │ 10.25                        │ 10.25                         │ -          │ 1              │
│ double                         │ 0                            │ 515214588.53                  │ 0                            │ 515214588.53                  │ -          │ 100            │
│ decimal                        │ 0                            │ 634690289.94                  │ 0                            │ 634690289.94                  │ -          │ 100            │
│ string                         │ A aut aperiam distinctio...  │ Voluptatem quo dolores...     │ A aut aperiam distinctio...  │ Voluptatem quo dolores...     │ -          │ 100            │
│ date                           │ 19361                        │ 19660                         │ 19361                        │ 19660                         │ -          │ 85             │
│ datetime                       │ 1672586016000000             │ 1698912601000000              │ 1672586016000000             │ 1698912601000000              │ -          │ 100            │
│ time                           │ 7200000000                   │ 7200000001                    │ 7200000000                   │ 7200000001                    │ -          │ 2              │
│ list_of_datetimes.list.element │ 1672577279000000             │ 1699336548000000              │ 1672577279000000             │ 1699336548000000              │ -          │ 300            │
│ map_of_ints.key_value.key      │ a                            │ c                             │ a                            │ c                             │ -          │ 3              │
│ map_of_ints.key_value.value    │ 369858                       │ 2145864542                    │ 369858                       │ 2145864542                    │ -          │ 300            │
│ list_of_strings.list.element   │ A nesciunt autem nesciunt... │ Voluptatum voluptas maxime... │ A nesciunt autem nesciunt... │ Voluptatum voluptas maxime... │ -          │ 691            │
│ struct_flat.id                 │ 1                            │ 100                           │ 1                            │ 100                           │ -          │ 100            │
│ struct_flat.name               │ name_00001                   │ name_00100                    │ name_00001                   │ name_00100                    │ -          │ 100            │
└────────────────────────────────┴──────────────────────────────┴──────────────────────── Total: 16 ───────────────────────────┴───────────────────────────────┴────────────┴────────────────┘

┌────────────────────────────────┬─────────────────┬──────────────── Page Headers ──────┬───────────────────┬───────────────────────┬─────────────────┐
│ path                           │ type            │ encoding         │ compressed size │ uncompressed size │ dictionary num values │ data num values │
├────────────────────────────────┼─────────────────┼──────────────────┼─────────────────┼───────────────────┼───────────────────────┼─────────────────┤
│ boolean                        │ DATA_PAGE       │ PLAIN            │ 20              │ 20                │ -                     │ 100             │
│ int32                          │ DATA_PAGE       │ PLAIN            │ 407             │ 407               │ -                     │ 100             │
│ int64                          │ DATA_PAGE       │ PLAIN            │ 807             │ 807               │ -                     │ 100             │
│ float                          │ DICTIONARY_PAGE │ PLAIN_DICTIONARY │ 4               │ 4                 │ 1                     │ -               │
│ double                         │ DATA_PAGE       │ PLAIN            │ 807             │ 807               │ -                     │ 100             │
│ decimal                        │ DATA_PAGE       │ PLAIN            │ 507             │ 507               │ -                     │ 100             │
│ string                         │ DATA_PAGE       │ PLAIN            │ 4,227           │ 4,227             │ -                     │ 100             │
│ date                           │ DATA_PAGE       │ PLAIN            │ 407             │ 407               │ -                     │ 100             │
│ datetime                       │ DATA_PAGE       │ PLAIN            │ 807             │ 807               │ -                     │ 100             │
│ time                           │ DICTIONARY_PAGE │ PLAIN_DICTIONARY │ 16              │ 16                │ 2                     │ -               │
│ list_of_datetimes.list.element │ DATA_PAGE       │ PLAIN            │ 2,487           │ 2,487             │ -                     │ 300             │
│ map_of_ints.key_value.key      │ DICTIONARY_PAGE │ PLAIN_DICTIONARY │ 15              │ 15                │ 3                     │ -               │
│ map_of_ints.key_value.value    │ DATA_PAGE       │ PLAIN            │ 1,287           │ 1,287             │ -                     │ 300             │
│ list_of_strings.list.element   │ DATA_PAGE       │ PLAIN            │ 28,345          │ 28,345            │ -                     │ 691             │
│ struct_flat.id                 │ DATA_PAGE       │ PLAIN            │ 407             │ 407               │ -                     │ 100             │
│ struct_flat.name               │ DATA_PAGE       │ PLAIN            │ 1,407           │ 1,407             │ -                     │ 100             │
└────────────────────────────────┴─────────────────┴────────────────── Total: 16 ───────┴───────────────────┴───────────────────────┴─────────────────┘
OUTPUT,
            $tester->getDisplay()
        );
        $this->assertSame(0, $tester->getStatusCode());
    }
}
