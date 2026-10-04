<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Physical;

use Flow\ETL\Column\Physical\XmlCharacterReferences;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;

final class XmlCharacterReferencesTest extends FlowTestCase
{
    #[TestWith(['<row a="&#x17C;&#xF3;&#x142;&#x107;"/>', '<row a="żółć"/>'])]
    #[TestWith(['<row a="&#380;"/>', '<row a="ż"/>'])]
    #[TestWith(['<row a="&#x1F600;"/>', '<row a="😀"/>'])]
    #[TestWith(['<row a="&#x3C;&#60;&#x26;"/>', '<row a="&#x3C;&#60;&#x26;"/>'])]
    #[TestWith(['<row><![CDATA[&#x17C;]]></row>', '<row><![CDATA[&#x17C;]]></row>'])]
    #[TestWith(['<row><!-- &#x17C; --></row>', '<row><!-- &#x17C; --></row>'])]
    #[TestWith(['<row a="ż">&amp;</row>', '<row a="ż">&amp;</row>'])]
    public function test_decodes_non_ascii_references_outside_cdata_and_comments(string $xml, string $decoded): void
    {
        static::assertSame($decoded, (new XmlCharacterReferences())->decoded($xml));
    }
}
