<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use function Flow\ETL\DSL\{html_entry, html_schema, str_entry};
use Dom\HTMLDocument;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\HTMLEntry;
use Flow\ETL\Schema\Metadata;
use PHPUnit\Framework\Attributes\{DataProvider, RequiresPhp};
use PHPUnit\Framework\TestCase;

#[RequiresPhp('>= 8.4')]
final class HTMLEntryTest extends TestCase
{
    public static function is_equal_data_provider() : \Generator
    {
        /* @phpstan-ignore-next-line */
        $doc1 = HTMLDocument::createFromString('<!DOCTYPE html><html lang="en"><head></head><body><div>2</div><p>3</p></body></html>');
        /* @phpstan-ignore-next-line */
        $doc2 = HTMLDocument::createFromString('<!DOCTYPE html><html lang="en"><head></head><body><div>2</div><p>3</p></body></html>');

        yield 'equal names and equal simple html documents' => [
            true,
            html_entry('name', $doc1),
            html_entry('name', $doc2),
        ];

        /* @phpstan-ignore-next-line */
        $doc1 = HTMLDocument::createFromString('<!DOCTYPE html><html lang="en"><head></head><body><div id="id">2</div><p>3</p></body></html>');
        /* @phpstan-ignore-next-line */
        $doc2 = HTMLDocument::createFromString('<!DOCTYPE html><html lang="en"><head></head><body><div>2</div><p id="id">3</p></body></html>');

        yield 'equal names and equal simple html documents with different order of attributes' => [
            false,
            html_entry('name', $doc1),
            html_entry('name', $doc2),
        ];

        /* @phpstan-ignore-next-line */
        $doc1 = HTMLDocument::createFromString('<!DOCTYPE html><html lang="en"><head></head><body><div id="foo">2</div><p>3</p></body></html>');
        /* @phpstan-ignore-next-line */
        $doc2 = HTMLDocument::createFromString('<!DOCTYPE html><html lang="en"><head></head><body><div id="bar">2</div><p>3</p></body></html>');

        yield 'equal nodes but different attributes' => [
            false,
            html_entry('name', $doc1),
            html_entry('name', $doc2),
        ];

        /* @phpstan-ignore-next-line */
        $doc1 = HTMLDocument::createFromString('<!DOCTYPE html><html lang="en"><head></head><body><div id="id">2</div><p>3</p></body></html>');
        /* @phpstan-ignore-next-line */
        $doc2 = HTMLDocument::createFromString('<!DOCTYPE html><html lang="en"><head></head><body><p>3</p></body></html>');

        yield 'equal attributes but different nodes' => [
            false,
            html_entry('name', $doc1),
            html_entry('name', $doc2),
        ];

        /* @phpstan-ignore-next-line */
        $doc1 = HTMLDocument::createFromString('<!DOCTYPE html><html lang="en"><head></head><body><div>2</div><p>3</p></body></html>');
        /* @phpstan-ignore-next-line */
        $doc2 = HTMLDocument::createFromString('<!DOCTYPE html><html lang="en"><head></head><body><div>2</div><p>3</p></body></html>');

        yield 'different names and equal simple html documents' => [
            false,
            html_entry('name', $doc1),
            html_entry('other-name', $doc2),
        ];

        /* @phpstan-ignore-next-line */
        $doc1 = HTMLDocument::createFromString('<!DOCTYPE html><html lang="en"><head></head><body><div>2</div><p>3</p></body></html>');

        yield 'different types' => [
            false,
            html_entry('name', $doc1),
            str_entry('other-name', '<!DOCTYPE html><html lang="en"><head></head><body><div>2</div><p>3</p></body></html>'),
        ];
    }

    public function test_canonicalization() : void
    {
        $doc = '<!DOCTYPE html><html lang="en"><head></head><body><div id="foo">2</div><p>3</p></body></html>';
        $doc2 = <<<'HTML'
<!DOCTYPE html>
<html lang="en">
<head></head>
<body>
    <div id="foo">2</div>
    <p>3</p>
</body>
</html>
HTML;

        self::assertNotEquals(
            html_entry('row', $doc)->toString(),
            html_entry('row', $doc2)->toString(),
        );
    }

    public function test_creating_entry_from_valid_html_string() : void
    {
        $html = '<!DOCTYPE html><html lang="en"><head></head><body><div id="id">2</div><p>3</p></body></html>';

        $entry = html_entry('name', $html);

        self::assertSame('name', $entry->name());
        self::assertSame($html, $entry->__toString());
    }

    public function test_definition() : void
    {
        self::assertEquals(
            html_schema('html'),
            (html_entry('html', '<!DOCTYPE html><html lang="en"><head></head><body><div>baz</div></body></html>'))->definition()
        );
    }

    public function test_duplicating_entry() : void
    {
        $entry = html_entry('html', <<<'HTML'
<!DOCTYPE html>
<html lang="en">
<head></head>
<body>
    <div id="foo">2</div>
    <p>3</p>
</body>
</html>
HTML);
        $duplicated = $entry->duplicate();

        self::assertNotSame($entry, $duplicated);
        self::assertEquals($entry, $duplicated);
    }

    /**
     * @param Entry<mixed> $nextEntry
     */
    #[DataProvider('is_equal_data_provider')]
    public function test_is_equal(bool $equals, HTMLEntry $entry, Entry $nextEntry) : void
    {
        self::assertSame($equals, $entry->isEqual($nextEntry));
    }

    public function test_map() : void
    {
        $entry = html_entry('entry-name', '<!DOCTYPE html><html lang="en"><head></head><body><div>baz</div></body></html>');

        self::assertEquals(
            $entry,
            $entry->map(static fn ($value) => $value)
        );
    }

    public function test_rename_preserves_metadata() : void
    {
        $metadata = Metadata::fromArray(['description' => 'test metadata', 'priority' => 1]);
        $entry = new HTMLEntry('old_name', '<!DOCTYPE html><html lang="en"><head></head><body><div>test</div></body></html>', $metadata);

        $renamedEntry = $entry->rename('new_name');

        self::assertSame('new_name', $renamedEntry->name());
        self::assertEquals($entry->value()?->saveHtml(), $renamedEntry->value()?->saveHtml());
        self::assertTrue($renamedEntry->definition()->metadata()->isEqual($metadata));
    }

    public function test_renames_entry() : void
    {
        $entry = html_entry('entry-name', '<!DOCTYPE html><html lang="en"><head></head><body><div>bar</div></body></html>');
        $newEntry = $entry->rename('new-entry-name');

        self::assertEquals('new-entry-name', $newEntry->name());
        self::assertEquals($entry->value(), $newEntry->value());
        self::assertEquals($entry->type(), $newEntry->type());
    }

    public function test_with_non_fully_valid_html_string() : void
    {
        $invalidHtml = <<<'HTML'
<!DOCTYPE html>
<html lang="en">
<head></head>
<body>
    <div>foo</div>
    <div><p><span>bar</span></span></p></div>
</body>
</html>
HTML;

        $validHtml = <<<'HTML'
<!DOCTYPE html>
<html lang="en">
<head></head>
<body>
    <div>foo</div>
    <div><p><span>bar</span></p></div>
</body>
</html>
HTML;

        $entry = html_entry('html', $invalidHtml);

        self::assertHtml($invalidHtml, $entry->toString(), false);
        self::assertHtml($validHtml, $entry->toString(), true);
    }

    public function test_with_value() : void
    {
        $entry = html_entry('html', '<!DOCTYPE html><html lang="en"><head></head><body><div>foobar</div></body></html>');

        /* @phpstan-ignore-next-line */
        $html = HTMLDocument::createFromString('<!DOCTYPE html><html lang="en"><head></head><body><div>different</div></body></html>');

        $newEntry = $entry->withValue($html);

        self::assertNotEquals($entry->toString(), $newEntry->toString());
        self::assertEquals($html, $newEntry->value());
    }

    private function assertHtml(string $expected, string $html, bool $equals) : void
    {
        $expected = \preg_replace('/\s*/', '', $expected);
        $html = \preg_replace('/\s*/', '', $html);

        if ($equals) {
            self::assertEquals($expected, $html);
        } else {
            self::assertNotEquals($expected, $html);
        }
    }
}
