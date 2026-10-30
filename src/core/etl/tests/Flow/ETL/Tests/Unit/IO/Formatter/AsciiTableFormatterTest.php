<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Formatter;

use Flow\ETL\IO\Formatter\AsciiTableFormatter;
use Flow\ETL\Tests\Context\CommandOutputNormalizer;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;
use function array_slice;
use function array_unique;
use function explode;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function range;

final class AsciiTableFormatterTest extends FlowTestCase
{
    public function test_a_table_with_mb_strings(): void
    {
        $rows = array_to_rows([
            ['row' => '[498][534]/Wiele z tego,|/co niegdyś było, przepadło.'],
            ['row' => '[540][572]/A nie żyje już nikt z tych,|/którzy by o tym pamiętali.'],
            ['row' => '[572][647]WŁADCA PIERŚCIENI'],
            ['row' => '[701][741]/Wszystko zaczęło się|/od wykucia Pierścieni Władzy.'],
            ['row' => '[742][762]/Trzy zostały dane elfom...'],
            ['row' => '[763][805]/nieśmiertelnym, najmędrszym|/i najbliższym magii spośród wszystkich ras.'],
            ['row' => '[816][853]/Siedem - władcom krasnoludów,|/wspaniałym górnikom'],
        ], schema(str_schema('row')));

        CommandOutputNormalizer::assertCommandOutputContains(<<<'TABLE'
            +-------------------------------------------------------------------------------------+
            |                                                                                 row |
            +-------------------------------------------------------------------------------------+
            |                               [498][534]/Wiele z tego,|/co niegdyś było, przepadło. |
            |                  [540][572]/A nie żyje już nikt z tych,|/którzy by o tym pamiętali. |
            |                                                         [572][647]WŁADCA PIERŚCIENI |
            |                      [701][741]/Wszystko zaczęło się|/od wykucia Pierścieni Władzy. |
            |                                               [742][762]/Trzy zostały dane elfom... |
            | [763][805]/nieśmiertelnym, najmędrszym|/i najbliższym magii spośród wszystkich ras. |
            |                       [816][853]/Siedem - władcom krasnoludów,|/wspaniałym górnikom |
            +-------------------------------------------------------------------------------------+
            TABLE, (new AsciiTableFormatter())->format($rows, false));
    }

    public function test_a_table_with_mb_strings_truncate(): void
    {
        $rows = array_to_rows([
            ['row' => '[498][534]/Wiele z tego,|/co niegdyś było, przepadło.'],
            ['row' => '[540][572]/A nie żyje już nikt z tych,|/którzy by o tym pamiętali.'],
            ['row' => '[572][647]WŁADCA PIERŚCIENI'],
            ['row' => '[701][741]/Wszystko zaczęło się|/od wykucia Pierścieni Władzy.'],
            ['row' => '[742][762]/Trzy zostały dane elfom...'],
            ['row' => '[763][805]/nieśmiertelnym, najmędrszym|/i najbliższym magii spośród wszystkich ras.'],
            ['row' => '[816][853]/Siedem - władcom krasnoludów,|/wspaniałym górnikom'],
        ], schema(str_schema('row')));

        CommandOutputNormalizer::assertCommandOutputContains(<<<'TABLE'
            +----------------------+
            |                  row |
            +----------------------+
            | [498][534]/Wiele z t |
            | [540][572]/A nie żyj |
            | [572][647]WŁADCA PIE |
            | [701][741]/Wszystko  |
            | [742][762]/Trzy zost |
            | [763][805]/nieśmiert |
            | [816][853]/Siedem -  |
            +----------------------+
            TABLE, (new AsciiTableFormatter())->format($rows, true));
    }

    public function test_a_table_with_non_symmetric_entries(): void
    {
        $rows = array_to_rows(
            [
                ['row' => '[498][534]/Wiele z tego,|/co niegdyś było, przepadło.'],
                ['row' => '[540][572]/A nie żyje już nikt z tych,|/którzy by o tym pamiętali.'],
                ['row' => '[572][647]WŁADCA PIERŚCIENI'],
                ['row' => '[701][741]/Wszystko zaczęło się|/od wykucia Pierścieni Władzy.'],
                ['row' => '[742][762]/Trzy zostały dane elfom...'],
                ['row' => '[763][805]/nieśmiertelnym, najmędrszym|/i najbliższym magii spośród wszystkich ras.'],
                ['test' => '[816][853]/Siedem - władcom krasnoludów,|/wspaniałym górnikom'],
            ],
            schema(str_schema('row', nullable: true), str_schema('test', nullable: true)),
        );

        CommandOutputNormalizer::assertCommandOutputContains(<<<'TABLE'
            +-------------------------------------------------------------------------------------+---------------------------------------------------------------+
            |                                                                                 row |                                                          test |
            +-------------------------------------------------------------------------------------+---------------------------------------------------------------+
            |                               [498][534]/Wiele z tego,|/co niegdyś było, przepadło. |                                                               |
            |                  [540][572]/A nie żyje już nikt z tych,|/którzy by o tym pamiętali. |                                                               |
            |                                                         [572][647]WŁADCA PIERŚCIENI |                                                               |
            |                      [701][741]/Wszystko zaczęło się|/od wykucia Pierścieni Władzy. |                                                               |
            |                                               [742][762]/Trzy zostały dane elfom... |                                                               |
            | [763][805]/nieśmiertelnym, najmędrszym|/i najbliższym magii spośród wszystkich ras. |                                                               |
            |                                                                                     | [816][853]/Siedem - władcom krasnoludów,|/wspaniałym górnikom |
            +-------------------------------------------------------------------------------------+---------------------------------------------------------------+
            TABLE, (new AsciiTableFormatter())->format($rows, false));
    }

    public function test_a_table_with_single_row(): void
    {
        CommandOutputNormalizer::assertCommandOutputContains(<<<'TABLE'
            +----+------+
            | id | name |
            +----+------+
            |  1 |   EN |
            |  2 |   PL |
            +----+------+
            TABLE, (new AsciiTableFormatter())->format(
            array_to_rows(
                [['id' => 1, 'name' => 'EN'], ['id' => 2, 'name' => 'PL']],
                schema(int_schema('id'), str_schema('name')),
            ),
            false,
        ));
    }

    public function test_a_batch_of_2000_rows_prints_every_row_at_one_width_per_column(): void
    {
        $output = (new AsciiTableFormatter())->format(array_to_rows(
            array_map(static fn(int $id): array => ['id' => $id, 'name' => 'name-' . $id], range(1, 2000)),
            schema(int_schema('id'), str_schema('name')),
        ));
        $lines = explode(PHP_EOL, $output);

        static::assertCount(2000 + 4 + 2, $lines);
        static::assertSame('|   id |      name |', $lines[1]);
        static::assertSame('| 2000 | name-2000 |', $lines[2002]);
        static::assertSame('2000 rows', $lines[2004]);
        static::assertCount(1, array_unique(array_map('mb_strlen', array_slice($lines, 0, 2004))));
    }
}
