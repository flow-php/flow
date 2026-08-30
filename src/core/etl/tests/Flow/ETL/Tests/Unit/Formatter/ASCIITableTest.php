<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Formatter;

use Flow\ETL\Formatter\ASCII\ASCIITable;
use Flow\ETL\Tests\CommandOutputNormalizer;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class ASCIITableTest extends FlowTestCase
{
    use CommandOutputNormalizer;

    public function test_ascii_table_with_mb_strings(): void
    {
        $rows = rows(
            schema(str_schema('row')),
            row(['row' => '[498][534]/Wiele z tego,|/co niegdyś było, przepadło.']),
            row(['row' => '[540][572]/A nie żyje już nikt z tych,|/którzy by o tym pamiętali.']),
            row(['row' => '[572][647]WŁADCA PIERŚCIENI']),
            row(['row' => '[701][741]/Wszystko zaczęło się|/od wykucia Pierścieni Władzy.']),
            row(['row' => '[742][762]/Trzy zostały dane elfom...']),
            row(['row' => '[763][805]/nieśmiertelnym, najmędrszym|/i najbliższym magii spośród wszystkich ras.']),
            row(['row' => '[816][853]/Siedem - władcom krasnoludów,|/wspaniałym górnikom']),
        );

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
        $rows = rows(
            schema(str_schema('row')),
            row(['row' => '[498][534]/Wiele z tego,|/co niegdyś było, przepadło.']),
            row(['row' => '[540][572]/A nie żyje już nikt z tych,|/którzy by o tym pamiętali.']),
            row(['row' => '[572][647]WŁADCA PIERŚCIENI']),
            row(['row' => '[701][741]/Wszystko zaczęło się|/od wykucia Pierścieni Władzy.']),
            row(['row' => '[742][762]/Trzy zostały dane elfom...']),
            row(['row' => '[763][805]/nieśmiertelnym, najmędrszym|/i najbliższym magii spośród wszystkich ras.']),
            row(['row' => '[816][853]/Siedem - władcom krasnoludów,|/wspaniałym górnikom']),
        );

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
        $rows = rows(
            schema(str_schema('row', nullable: true), str_schema('test', nullable: true)),
            row(['row' => '[498][534]/Wiele z tego,|/co niegdyś było, przepadło.']),
            row(['row' => '[540][572]/A nie żyje już nikt z tych,|/którzy by o tym pamiętali.']),
            row(['row' => '[572][647]WŁADCA PIERŚCIENI']),
            row(['row' => '[701][741]/Wszystko zaczęło się|/od wykucia Pierścieni Władzy.']),
            row(['row' => '[742][762]/Trzy zostały dane elfom...']),
            row(['row' => '[763][805]/nieśmiertelnym, najmędrszym|/i najbliższym magii spośród wszystkich ras.']),
            row(['test' => '[816][853]/Siedem - władcom krasnoludów,|/wspaniałym górnikom']),
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
            TABLE, (new ASCIITable(rows(
            schema(int_schema('id'), str_schema('name')),
            row(['id' => 1, 'name' => 'EN']),
            row(['id' => 2, 'name' => 'PL']),
        )))->print(false));
    }
}
