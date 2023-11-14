<?php declare(strict_types=1);

namespace Flow\Dremel\Tests\Integration;

use Flow\Dremel\Dremel;
use PHPUnit\Framework\TestCase;

final class DremelTest extends TestCase
{
    public function test_deeply_nested_lists() : void
    {
        $values = [
            [
                [0, 1, 2],
            ],
            [
                [3, 4, 5],
                [3, 4, 5],
            ],
            [
                [6, 7, 8],
            ],
        ];

        $shredded = (new Dremel())->shred($values, 5);

        $this->assertSame(
            $values,
            (new Dremel())->assemble($shredded->repetitions, $shredded->definitions, $shredded->values)
        );
    }

    public function test_dremel_shredding_and_assembling() : void
    {
        $repetitions = [0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 0, 1, 1, 0, 1, 1, 1, 1];
        $definitions = [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2];
        $values = [['Suscipit officiis dolorum ea omnis est id magnam.', 'Ea rerum saepe a minima non iusto.'], ['Id dolor et et repellendus.', 'Cumque facilis aut quos et.', 'Sit illum ipsam dolor voluptatem est.'], ['Commodi dicta rerum quas omnis sunt dolor.', 'Architecto sint corrupti nihil soluta nesciunt.', 'Accusamus libero aliquam rerum.'], ['Eum molestias reiciendis cumque ad animi.', 'Sunt ad magnam quas dolores possimus sint aut.', 'Quidem cupiditate doloremque aut esse non.', 'Consequatur nobis delectus aut.', 'Quo fuga fugiat nulla dolor non fugit dolorum.', 'Voluptate ex culpa deleniti est eum qui.', 'Quia sunt quia ut consequatur et optio et.'], ['Aut soluta corrupti laborum qui.', 'Officia maiores natus voluptatem provident aut.', 'Voluptatem modi sequi molestiae aut molestiae.', 'Cumque qui voluptas quia.', 'Quis esse ut odio commodi quae.', 'Voluptatem est accusantium est et eum.', 'Ratione et ut fuga qui atque sed et.', 'Et aut ut quidem provident excepturi placeat.'], ['Rerum molestiae dicta libero dolorem.', 'Expedita fuga sequi a maiores quasi.', 'Nesciunt qui similique et.', 'Architecto perferendis qui sequi sint qui nemo.', 'Sequi in atque tenetur.', 'Voluptatem quod et placeat cupiditate.', 'Qui qui laborum consequatur quos cum totam.', 'Saepe sit quae eos accusamus.', 'Qui illum dolor vel consequuntur nihil.'], ['Vel tenetur velit quas.', 'Natus autem ab beatae nihil recusandae.', 'Ut quasi voluptatum qui dolore ut.', 'Ducimus et minima voluptatem cum sint non.', 'Rerum tenetur sunt quidem est et modi et.', 'Vitae sit eum eius rerum possimus.', 'Eos ipsa est a aliquid impedit doloremque nisi.', 'Aut illum quam sit asperiores.'], ['Repellat dolore sit ad amet sed repudiandae.', 'Quam nemo cum quo culpa.', 'Omnis sed minima vero.', 'Esse qui quo cumque earum eius nulla.', 'Sed in adipisci quas fuga.', 'Dolor est aliquid tempora.', 'Ut expedita id suscipit ut voluptatem.'], ['Sint ipsa et autem ut id vitae.', 'Sapiente ut ab qui.', 'Ullam sit numquam qui perferendis aut.'], ['Qui illum id nam quia quibusdam vero.', 'Quas laboriosam perferendis temporibus vero.', 'Numquam quas deserunt est et eius.', 'Voluptas debitis incidunt ea minus.', 'Pariatur ipsa ipsa sequi ut est dolor adipisci.']];

        $dremel = new Dremel();
        $shredded = $dremel->shred($values, \max($definitions));

        $this->assertSame($repetitions, $shredded->repetitions);
        $this->assertSame($definitions, $shredded->definitions);

        $assembledValues = $dremel->assemble($shredded->repetitions, $shredded->definitions, $shredded->values);

        $this->assertSame($values, $assembledValues);
    }

    public function test_dremel_shredding_and_assembling_list_with_empty_elements() : void
    {
        $repetitions = [0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1];
        $definitions = [3, 3, 3, 1, 3, 3, 3, 0, 2, 2, 2];
        $values = [[1, 2, 3], [], [4, 5, 6], null, [null, null, null]];

        $dremel = new Dremel();
        $shredded = $dremel->shred($values, 3);

        $this->assertSame($repetitions, $shredded->repetitions);
        $this->assertSame($definitions, $shredded->definitions);

        $this->assertSame(
            $values,
            $dremel->assemble($shredded->repetitions, $shredded->definitions, $shredded->values)
        );
    }

    public function test_dremel_shredding_and_assembling_list_with_nulls_in_list() : void
    {
        $repetitions = [0, 1, 1, 0, 1, 0, 1, 1];
        $definitions = [3, 3, 3, 2, 2, 3, 3, 3];
        $values = [[1, 2, 3], [null, null], [4, 5, 6]];

        $dremel = new Dremel();
        $shredded = $dremel->shred($values, 3);

        $this->assertSame($repetitions, $shredded->repetitions);
        $this->assertSame($definitions, $shredded->definitions);

        $this->assertSame(
            $values,
            $dremel->assemble($shredded->repetitions, $shredded->definitions, $shredded->values)
        );
    }

    public function test_dremel_shredding_and_assembling_nullable_nested_values() : void
    {
        $repetitions = [0, 1, 0, 0, 1, 1, 1, 1, 1, 0, 0, 1, 0, 0, 1, 1, 1, 1, 0, 0, 1, 1, 1, 1, 0];
        $definitions = [2, 2, 0, 2, 2, 2, 2, 2, 2, 0, 2, 2, 0, 2, 2, 2, 2, 2, 0, 2, 2, 2, 2, 2, 0];
        $values = [[0, 1], null, [0, 1, 2, 3, 4, 5], null, [0, 1], null, [0, 1, 2, 3, 4], null, [0, 1, 2, 3, 4], null];
        $flatValues = [0, 1, 0, 1, 2, 3, 4, 5, 0, 1, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4];

        $dremel = new Dremel();
        $shredded = $dremel->shred($values, 2);

        $this->assertSame($repetitions, $shredded->repetitions);
        $this->assertSame($definitions, $shredded->definitions);
        $this->assertSame($flatValues, $shredded->values);

        $this->assertSame(
            $values,
            $dremel->assemble($shredded->repetitions, $shredded->definitions, $shredded->values)
        );
    }
}
