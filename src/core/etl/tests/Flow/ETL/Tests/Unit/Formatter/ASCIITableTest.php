<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Formatter;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Formatter\ASCII\ASCIITable;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class ASCIITableTest extends TestCase
{
    public function test_ascii_table_with_mb_strings() : void
    {
        $rows = new Rows(
            Row::create(Entry::string('row', '[498][534]/Wiele z tego,|/co niegdyś było, przepadło.')),
            Row::create(Entry::string('row', '[540][572]/A nie żyje już nikt z tych,|/którzy by o tym pamiętali.')),
            Row::create(Entry::string('row', '[572][647]WŁADCA PIERŚCIENI')),
            Row::create(Entry::string('row', '[701][741]/Wszystko zaczęło się|/od wykucia Pierścieni Władzy.')),
            Row::create(Entry::string('row', '[742][762]/Trzy zostały dane elfom...')),
            Row::create(Entry::string('row', '[763][805]/nieśmiertelnym, najmędrszym|/i najbliższym magii spośród wszystkich ras.')),
            Row::create(Entry::string('row', '[816][853]/Siedem - władcom krasnoludów,|/wspaniałym górnikom')),
        );

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
            (new ASCIITable($rows))->print(false)
        );
    }

    public function test_ascii_table_with_mb_strings_truncate() : void
    {
        $rows = new Rows(
            Row::create(Entry::string('row', '[498][534]/Wiele z tego,|/co niegdyś było, przepadło.')),
            Row::create(Entry::string('row', '[540][572]/A nie żyje już nikt z tych,|/którzy by o tym pamiętali.')),
            Row::create(Entry::string('row', '[572][647]WŁADCA PIERŚCIENI')),
            Row::create(Entry::string('row', '[701][741]/Wszystko zaczęło się|/od wykucia Pierścieni Władzy.')),
            Row::create(Entry::string('row', '[742][762]/Trzy zostały dane elfom...')),
            Row::create(Entry::string('row', '[763][805]/nieśmiertelnym, najmędrszym|/i najbliższym magii spośród wszystkich ras.')),
            Row::create(Entry::string('row', '[816][853]/Siedem - władcom krasnoludów,|/wspaniałym górnikom')),
        );

        $this->assertStringContainsString(
            <<<'TABLE'
+--------------------+
|                 row|
+--------------------+
|[498][534]/Wiele z t|
| [540][572]/A nie ży|
| [572][647]WŁADCA PI|
|[701][741]/Wszystko |
|[742][762]/Trzy zost|
| [763][805]/nieśmier|
|[816][853]/Siedem - |
+--------------------+
TABLE,
            (new ASCIITable($rows))->print(true)
        );
    }

    public function test_ascii_table_with_non_symmetric_entries() : void
    {
        $rows = new Rows(
            Row::create(Entry::string('row', '[498][534]/Wiele z tego,|/co niegdyś było, przepadło.')),
            Row::create(Entry::string('row', '[540][572]/A nie żyje już nikt z tych,|/którzy by o tym pamiętali.')),
            Row::create(Entry::string('row', '[572][647]WŁADCA PIERŚCIENI')),
            Row::create(Entry::string('row', '[701][741]/Wszystko zaczęło się|/od wykucia Pierścieni Władzy.')),
            Row::create(Entry::string('row', '[742][762]/Trzy zostały dane elfom...')),
            Row::create(Entry::string('row', '[763][805]/nieśmiertelnym, najmędrszym|/i najbliższym magii spośród wszystkich ras.')),
            Row::create(Entry::string('test', '[816][853]/Siedem - władcom krasnoludów,|/wspaniałym górnikom')),
        );

        $this->assertStringContainsString(
            <<<'TABLE'
+-----------------------------------------------------------------------------------+-------------------------------------------------------------+
|                                                                                row|                                                         test|
+-----------------------------------------------------------------------------------+-------------------------------------------------------------+
|                              [498][534]/Wiele z tego,|/co niegdyś było, przepadło.|                                                             |
|                 [540][572]/A nie żyje już nikt z tych,|/którzy by o tym pamiętali.|                                                             |
|                                                        [572][647]WŁADCA PIERŚCIENI|                                                             |
|                     [701][741]/Wszystko zaczęło się|/od wykucia Pierścieni Władzy.|                                                             |
|                                              [742][762]/Trzy zostały dane elfom...|                                                             |
|[763][805]/nieśmiertelnym, najmędrszym|/i najbliższym magii spośród wszystkich ras.|                                                             |
|                                                                                   |[816][853]/Siedem - władcom krasnoludów,|/wspaniałym górnikom|
+-----------------------------------------------------------------------------------+-------------------------------------------------------------+
TABLE,
            (new ASCIITable($rows))->print(false)
        );
    }

    public function test_ascii_table_with_single_row() : void
    {
        $table = [
            ['id' => 1, 'name' => 'EN'],
        ];

        $this->assertStringContainsString(
            <<<'TABLE'
+--+----+
|id|name|
+--+----+
| 1|  EN|
| 2|  PL|
+--+----+
TABLE,
            (new ASCIITable(new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('name', 'EN')),
                Row::create(Entry::integer('id', 2), Entry::string('name', 'PL')),
            )))->print(false)
        );
    }
}
