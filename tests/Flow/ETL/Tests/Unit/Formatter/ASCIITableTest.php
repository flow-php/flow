<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Formatter;

use Flow\ETL\Formatter\ASCII\ASCIITable;
use PHPUnit\Framework\TestCase;

final class ASCIITableTest extends TestCase
{
    public function test_ascii_table_with_mb_strings() : void
    {
        $table = [
            ['row' => '[498][534]/Wiele z tego,|/co niegdyś było, przepadło.'],
            ['row' => '[540][572]/A nie żyje już nikt z tych,|/którzy by o tym pamiętali.'],
            ['row' => '[572][647]WŁADCA PIERŚCIENI'],
            ['row' => '[701][741]/Wszystko zaczęło się|/od wykucia Pierścieni Władzy.'],
            ['row' => '[742][762]/Trzy zostały dane elfom...'],
            ['row' => '[763][805]/nieśmiertelnym, najmędrszym|/i najbliższym magii spośród wszystkich ras.'],
            ['row' => '[816][853]/Siedem - władcom krasnoludów,|/wspaniałym górnikom'],
        ];

        $this->assertStringContainsString(
            <<<'TABLE'
+-----------------------------------------------------------------------------------+
|                                                                                row|
+-----------------------------------------------------------------------------------+
|                              [498][534]/Wiele z tego,|/co niegdyś było, przepadło.|
|                 [540][572]/A nie żyje już nikt z tych,|/którzy by o tym pamiętali.|
|                                                        [572][647]WŁADCA PIERŚCIENI|
|                     [701][741]/Wszystko zaczęło się|/od wykucia Pierścieni Władzy.|
|                                              [742][762]/Trzy zostały dane elfom...|
|[763][805]/nieśmiertelnym, najmędrszym|/i najbliższym magii spośród wszystkich ras.|
|                      [816][853]/Siedem - władcom krasnoludów,|/wspaniałym górnikom|
+-----------------------------------------------------------------------------------+
TABLE,
            (new ASCIITable())->makeTable($table, false)
        );
    }

    public function test_ascii_table_with_mb_strings_truncate() : void
    {
        $table = [
            ['row' => '[498][534]/Wiele z tego,|/co niegdyś było, przepadło.'],
            ['row' => '[540][572]/A nie żyje już nikt z tych,|/którzy by o tym pamiętali.'],
            ['row' => '[572][647]WŁADCA PIERŚCIENI'],
            ['row' => '[701][741]/Wszystko zaczęło się|/od wykucia Pierścieni Władzy.'],
            ['row' => '[742][762]/Trzy zostały dane elfom...'],
            ['row' => '[763][805]/nieśmiertelnym, najmędrszym|/i najbliższym magii spośród wszystkich ras.'],
            ['row' => '[816][853]/Siedem - władcom krasnoludów,|/wspaniałym górnikom'],
        ];

        $this->assertStringContainsString(
            <<<'TABLE'
+--------------------+
|                 row|
+--------------------+
|[498][534]/Wiele ...|
|[540][572]/A nie ...|
|[572][647]WŁADCA ...|
|[701][741]/Wszyst...|
|[742][762]/Trzy z...|
|[763][805]/nieśmi...|
|[816][853]/Siedem...|
+--------------------+
TABLE,
            (new ASCIITable())->makeTable($table, true)
        );
    }
}
