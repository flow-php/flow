<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Unit;

use Flow\ETL\Adapter\XML\RootlessDocument;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Stream\StringSourceStream;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\Filesystem\DSL\path;

final class RootlessDocumentTest extends FlowTestCase
{
    /**
     * @return Generator<string, array{string, int<1, max>}>
     */
    public static function rootlessDocuments(): Generator
    {
        foreach ([
            'an empty document' => '',
            'whitespace' => " \n\t\r ",
            'a BOM' => "\xEF\xBB\xBF",
            'a BOM and a declaration' => "\xEF\xBB\xBF<?xml version='1.0'?>",
            'a comment' => '<!-- c -->',
            'a comment holding closer look-alikes' => '<!-- a - b -> -- ->-->',
            'a processing instruction holding closer look-alikes' => '<?pi a ? b > ?>',
            'a declaration, comments and processing instructions' => "<?xml version='1.0'?>\n<!-- a -->\n<?pi x?><!--b-->\n",
        ] as $label => $xml) {
            yield $label . ' read a byte at a time' => [$xml, 1];
            yield $label . ' read at once' => [$xml, 8192];
        }
    }

    /**
     * @return Generator<string, array{string, int<1, max>}>
     */
    public static function documentsWithMore(): Generator
    {
        foreach ([
            'a root' => '<root/>',
            'text' => 'junk',
            'a BOM and a root' => "\xEF\xBB\xBF<root/>",
            'a comment and text' => '<!-- c -->junk',
            'a comment and a root' => '<!-- c --><root/>',
            'a doctype' => '<!DOCTYPE root>',
            'an unterminated comment' => '<!-- c -',
            'an unterminated processing instruction' => '<?pi x?',
            'a lone opening bracket' => '<',
            'a cut comment opener' => '<!-',
            'a cut BOM' => "\xEF\xBB",
            'a BOM after whitespace' => " \xEF\xBB\xBF",
            'a second BOM' => "\xEF\xBB\xBF\xEF\xBB\xBF",
        ] as $label => $xml) {
            yield $label . ' read a byte at a time' => [$xml, 1];
            yield $label . ' read at once' => [$xml, 8192];
        }
    }

    /**
     * @param int<1, max> $bufferSize
     */
    #[DataProvider('rootlessDocuments')]
    public function test_a_document_without_a_root_matches(string $xml, int $bufferSize): void
    {
        static::assertTrue((new RootlessDocument())->matches(
            new StringSourceStream(path('memory://a.xml'), $xml),
            $bufferSize,
        ));
    }

    /**
     * @param int<1, max> $bufferSize
     */
    #[DataProvider('documentsWithMore')]
    public function test_a_document_with_more_does_not_match(string $xml, int $bufferSize): void
    {
        static::assertFalse((new RootlessDocument())->matches(
            new StringSourceStream(path('memory://a.xml'), $xml),
            $bufferSize,
        ));
    }
}
