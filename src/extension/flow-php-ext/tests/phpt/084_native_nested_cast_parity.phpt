--TEST--
NativeColumnBuilder casts random nested values, raw and refused forms, as the PHP builder does through append() and appendMany()
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Column\DefaultBackend;
use Flow\ETL\Column\PhpBackend;

use function Flow\ETL\DSL\definition_from_type;

mt_srand(84);
$identical = 0;
$refused = 0;
$nulls = 0;
$cases = 0;

for ($t = 0; $t < 400; $t++) {
    [$type, $generate] = write_random_type(mt_rand(1, 3));
    $definition = definition_from_type('v', $type);
    $values = [];

    for ($i = mt_rand(1, 5); $i > 0; $i--) {
        $values[] = cast_random_input($type, $generate());
    }

    foreach ([false, true] as $many) {
        $column = static fn(object $backend): Closure => static function () use ($backend, $definition, $values, $many): object {
            $builder = $backend->builder($definition);

            if ($many) {
                $builder->appendMany($values);
            } else {
                foreach ($values as $value) {
                    $builder->append($value);
                }
            }

            return $builder->finish();
        };

        foreach (['values', 'physicals', 'encode'] as $read) {
            $cases++;
            $php = outcome(static fn(): mixed => $column(new PhpBackend())()->{$read}());
            $native = outcome(static fn(): mixed => $column(new DefaultBackend())()->{$read}());

            if ($php !== $native) {
                echo ($many ? 'appendMany ' : 'append ') . "{$read} {$type->toString()} " . var_export($values, true) . "\n  php:    {$php}\n  native: {$native}\n";

                continue;
            }

            $identical++;
            $refused += refused($php) ? 1 : 0;
            $nulls += str_contains(serialize($values), 'N;') ? 1 : 0;
        }
    }
}

echo "{$identical} of {$cases} identical, {$refused} refused, {$nulls} with a null\n";
?>
--EXPECT--
2400 of 2400 identical, 690 refused, 654 with a null
