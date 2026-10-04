<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Formatter;

use Flow\ETL\Formatter\ASCII\ASCIITable;
use Flow\ETL\Tests\CommandOutputNormalizer;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class ASCIITableTest extends FlowTestCase
{
    use CommandOutputNormalizer;

    public function test_ascii_table_with_mb_strings(): void
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

        self::assertCommandOutputContains(<<<'TABLE'
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
            TABLE, (new ASCIITable($rows))->print(false));
    }

    public function test_ascii_table_with_mb_strings_truncate(): void
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

        self::assertCommandOutputContains(<<<'TABLE'
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
            TABLE, (new ASCIITable($rows))->print(true));
    }

    public function test_ascii_table_with_non_symmetric_entries(): void
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

        self::assertCommandOutputContains(<<<'TABLE'
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
            TABLE, (new ASCIITable($rows))->print(false));
    }

    public function test_ascii_table_with_single_row(): void
    {
        self::assertCommandOutputContains(<<<'TABLE'
            +----+------+
            | id | name |
            +----+------+
            |  1 |   EN |
            |  2 |   PL |
            +----+------+
            TABLE, (new ASCIITable(array_to_rows(
            [['id' => 1, 'name' => 'EN'], ['id' => 2, 'name' => 'PL']],
            schema(int_schema('id'), str_schema('name')),
        )))->print(false));
    }
}
