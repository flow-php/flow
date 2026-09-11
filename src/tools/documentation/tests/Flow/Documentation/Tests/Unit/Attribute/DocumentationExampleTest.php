<?php

declare(strict_types=1);

namespace Flow\Documentation\Tests\Unit\Attribute;

use Flow\Documentation\Attribute\DocumentationExample;
use PHPUnit\Framework\TestCase;
use ReflectionAttribute;
use ReflectionFunction;

use function array_map;

final class DocumentationExampleTest extends TestCase
{
    public function test_is_repeatable_on_a_function(): void
    {
        $examples = (new ReflectionFunction(#[DocumentationExample('reading', 'csv')]
        #[DocumentationExample('reading', 'json')] static fn() => null))->getAttributes(DocumentationExample::class);

        static::assertSame(
            ['csv', 'json'],
            array_map(
                /**
                 * @param ReflectionAttribute<DocumentationExample> $a
                 */
                static fn(ReflectionAttribute $a): string => $a->newInstance()->example,
                $examples,
            ),
        );
    }
}
