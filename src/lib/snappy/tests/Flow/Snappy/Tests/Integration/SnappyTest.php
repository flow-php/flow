<?php

declare(strict_types=1);

namespace Flow\Snappy\Tests\Integration;

use Faker\Factory;
use Flow\Snappy\Snappy;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\generate_random_int;
use function function_exists;
use function snappy_compress;
use function snappy_uncompress;

final class SnappyTest extends TestCase
{
    public function test_decompress_text_compressed_with_extension(): void
    {
        if (!function_exists('snappy_compress')) {
            static::markTestSkipped('Snappy extension is not installed');
        }

        $string = 'This is some random string with UTF-8 characters: ąęćźżół';

        $snappy = new Snappy();

        static::assertSame($string, $snappy->uncompress(snappy_compress($string)));
    }

    public function test_decompress_with_extension_text_compressed_with_library(): void
    {
        if (!function_exists('snappy_uncompress')) {
            static::markTestSkipped('Snappy extension is not installed');
        }

        $string = 'This is some random string with UTF-8 characters: ąęćźżół';

        $snappy = new Snappy();

        static::assertSame($string, snappy_uncompress($snappy->compress($string)));
    }

    public function test_snappy_compression(): void
    {
        $string = 'This is some random string';

        $snappy = new Snappy();

        static::assertSame($string, $snappy->uncompress($snappy->compress($string)));
    }

    public function test_snappy_compression_on_a_longer_text(): void
    {
        $string = 'Fuga dolorem cum ut voluptatem alias est. At et atque et voluptatem explicabo. Error rerum quia sit. Amet minima corporis occaecati. Numquam ea molestiae itaque est modi accusamus. Est totam iste et aut. Asperiores voluptatem occaecati quaerat omnis. Consequatur qui voluptas porro natus et fugit consectetur dolor. Iusto voluptatibus libero dolores reiciendis a. Aspernatur tempore sed veritatis modi quis dicta. Eos illum sed ipsum et voluptatum. Et vel perspiciatis magnam ut maiores vitae.';

        $snappy = new Snappy();

        static::assertSame($string, $snappy->uncompress($snappy->compress($string)));
    }

    public function test_snappy_compression_with_dynamically_generated_texts(): void
    {
        $snappy = new Snappy();

        for ($iteration = 0; $iteration < 100; $iteration++) {
            $string = Factory::create()->text(generate_random_int(10, 1000));

            static::assertSame(
                $string,
                $snappy->uncompress($snappy->compress($string)),
                'Snappy compression/decomression failed at ' . $iteration . ' iteration, with text: "' . $string . '"',
            );
        }
    }

    public function test_snappy_compression_with_utf_8_characters(): void
    {
        $string = 'This is some random string with UTF-8 characters: ąęćźżół';

        $snappy = new Snappy();

        static::assertSame($string, $snappy->uncompress($snappy->compress($string)));
    }
}
