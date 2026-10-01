<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

final class XmlNodeMother
{
    /**
     * Documents whose `rows/row` node is an edge of the node text: non-ASCII text, attributes, names, comments, PIs and
     * CDATA, entities, inherited namespaces, a Latin-1 source, whitespace; each with the node's outer XML.
     *
     * @return array<string, array{document: string, node: string}>
     */
    public static function edges(): array
    {
        return [
            'ascii' => ['document' => '<rows><row a="1"><b>x</b></row></rows>', 'node' => '<row a="1"><b>x</b></row>'],
            'non-ascii text' => [
                'document' => '<rows><row><a>zażółć gęślą jaźń ☃ 😀</a></row></rows>',
                'node' => '<row><a>zażółć gęślą jaźń ☃ 😀</a></row>',
            ],
            'non-ascii attribute' => ['document' => '<rows><row a="żółć"/></rows>', 'node' => '<row a="żółć"/>'],
            'non-ascii name' => [
                'document' => '<rows><row><żółć ść="1"/></row></rows>',
                'node' => '<row><żółć ść="1"/></row>',
            ],
            'non-ascii comment, pi, cdata' => [
                'document' => '<rows><row><!-- żółć --><?pi żółć?><![CDATA[żółć & <]]></row></rows>',
                'node' => '<row><!-- żółć --><?pi żółć?><![CDATA[żółć & <]]></row>',
            ],
            'entities' => [
                'document' => '<rows><row a="x &amp; &quot;y&quot; &#10; &#9;"><b>&lt;p&gt; &amp; &#169; &#x1F600; &#13;</b></row></rows>',
                'node' => '<row a="x &amp; &quot;y&quot; &#10; &#9;"><b>&lt;p&gt; &amp; © 😀 &#13;</b></row>',
            ],
            'inherited namespace' => [
                'document' => '<rows xmlns="urn:d" xmlns:p="urn:p"><row p:a="1"><p:b>ż</p:b></row></rows>',
                'node' => '<row xmlns="urn:d" xmlns:p="urn:p" p:a="1"><p:b>ż</p:b></row>',
            ],
            'latin1 source' => [
                'document' => "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?><rows><row><a>\xE9</a></row></rows>",
                'node' => '<row><a>é</a></row>',
            ],
            'whitespace and empty' => [
                'document' => "<rows><row>\n <a/>\t<b></b> </row></rows>",
                'node' => "<row>\n <a/>\t<b/> </row>",
            ],
        ];
    }
}
