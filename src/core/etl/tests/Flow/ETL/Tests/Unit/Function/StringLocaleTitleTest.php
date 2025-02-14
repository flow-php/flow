<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{ref, str_entry, type_string};
use Flow\ETL\Function\StringLocaleTitle;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Tests\FlowTestCase;
use Symfony\Component\String\AbstractUnicodeString;

final class StringLocaleTitleTest extends FlowTestCase
{
    public function test_returns_method_returns_string_type() : void
    {
        $stringTitleFunction = new StringLocaleTitle('str', 'en');
        $returnType = $stringTitleFunction->returns();

        self::assertInstanceOf(Type::class, $returnType);

        self::assertTrue($returnType->isEqual(type_string()));
    }

    public function test_string_locale_title_en() : void
    {
        /** @phpstan-ignore-next-line */
        if (method_exists(AbstractUnicodeString::class, 'localeTitle')) {
            self::assertSame(
                'Foo ijssel',
                ref('str')->stringLocaleTitle('en')->eval(
                    row(str_entry('str', 'foo ijssel'))
                )
            );
        } else {
            self::assertSame(
                'Foo ijssel',
                ref('str')->stringTitle()->eval(
                    row(str_entry('str', 'foo ijssel'))
                )
            );
        }
    }

    public function test_string_locale_title_nl() : void
    {
        /** @phpstan-ignore-next-line */
        if (method_exists(AbstractUnicodeString::class, 'localeTitle')) {
            self::assertSame(
                'Foo IJssel',
                ref('str')->stringLocaleTitle('nl')->eval(
                    row(str_entry('str', 'foo ijssel'))
                )
            );
        } else {
            self::assertNotSame(
                'Foo IJssel',
                ref('str')->stringTitle()->eval(
                    row(str_entry('str', 'foo ijssel'))
                )
            );
        }

    }
}
