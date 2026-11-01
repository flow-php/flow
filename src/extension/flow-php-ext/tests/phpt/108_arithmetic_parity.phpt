--TEST--
RustBackend::add(), subtract(), multiply() and modulo() answer exactly as PhpBackend over every pair of values, overflow, nulls and refused moduli included
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Column\RustBackend;
use Flow\ETL\Values\Schema\Definitions;
use Flow\Types\Type;

use function Flow\Types\DSL\{type_float, type_integer};

function operand(Backend $backend, Type $type, array $values): Column
{
    $builder = $backend->builder(Definitions::of('c', $type, true));
    $builder->appendMany($values);

    return $builder->finish();
}

function computed(callable $answer, string &$class): string
{
    try {
        $column = $answer();
        $class = get_class($column);

        return $column->type()->toString() . ' ' . var_export($column->values(), true);
    } catch (EvaluationException $e) {
        return get_class($e) . ' row ' . $e->rowIndex . ': ' . $e->getMessage();
    } catch (Throwable $e) {
        return get_class($e) . ': ' . $e->getMessage();
    }
}

$ints = [PHP_INT_MIN, -7, -1, 0, 1, 3, PHP_INT_MAX];
$moduli = [3, 1, PHP_INT_MAX];
$floats = [-INF, -1.5, -0.0, 0.0, 1.5, INF, NAN];
$pairs = [
    'integer and integer within range' => [type_integer(), [-3_037_000_499, -7, -1, 0, 1, 3, 3_037_000_499], type_integer(), [-3_037_000_499, -7, -1, 0, 1, 3, 3_037_000_499], ['add', 'subtract', 'multiply']],
    'integer and integer past the range' => [type_integer(), $ints, type_integer(), $ints, ['add', 'subtract', 'multiply']],
    'float and float' => [type_float(), $floats, type_float(), $floats, ['add', 'subtract', 'multiply']],
    'integer and float' => [type_integer(), [PHP_INT_MAX, -7, 0, 9_007_199_254_740_993], type_float(), [1.5, -0.0, NAN, INF], ['add', 'subtract', 'multiply']],
    'float and integer' => [type_float(), [1.5, -0.0, NAN, -INF], type_integer(), [PHP_INT_MIN, -7, 0, 1], ['add', 'subtract', 'multiply']],
    'modulo by positive moduli' => [type_integer(), $ints, type_integer(), $moduli, ['modulo']],
    'modulo by any int' => [type_integer(), $ints, type_integer(), $ints, ['modulo']],
];
$php = new PhpBackend();
$rust = new RustBackend();

foreach ($pairs as $label => [$leftType, $leftValues, $rightType, $rightValues, $methods]) {
    foreach (['' => [], ' with a null' => [null]] as $variant => $extra) {
        $lefts = [];
        $rights = [];

        foreach ([...$leftValues, ...$extra] as $left) {
            foreach ($rightValues as $right) {
                $lefts[] = $left;
                $rights[] = $right;
            }
        }

        foreach ($methods as $method) {
            $phpClass = $rustClass = '';
            $expected = computed(fn () => $php->{$method}(operand($php, $leftType, $lefts), operand($php, $rightType, $rights)), $phpClass);
            $actual = computed(fn () => $rust->{$method}(operand($rust, $leftType, $lefts), operand($rust, $rightType, $rights)), $rustClass);

            echo $label, $variant, ' ', $method, ': ', $expected === $actual
                ? 'same' . ($rustClass === '' ? ', refused' : ', ' . $rustClass)
                : "MISMATCH\n  php:  " . $expected . "\n  rust: " . $actual, "\n";
        }
    }
}
?>
--EXPECT--
integer and integer within range add: same, Flow\ETL\Values\Column\RustColumn
integer and integer within range subtract: same, Flow\ETL\Values\Column\RustColumn
integer and integer within range multiply: same, Flow\ETL\Values\Column\RustColumn
integer and integer within range with a null add: same, refused
integer and integer within range with a null subtract: same, refused
integer and integer within range with a null multiply: same, refused
integer and integer past the range add: same, refused
integer and integer past the range subtract: same, refused
integer and integer past the range multiply: same, refused
integer and integer past the range with a null add: same, refused
integer and integer past the range with a null subtract: same, refused
integer and integer past the range with a null multiply: same, refused
float and float add: same, Flow\ETL\Values\Column\RustColumn
float and float subtract: same, Flow\ETL\Values\Column\RustColumn
float and float multiply: same, Flow\ETL\Values\Column\RustColumn
float and float with a null add: same, refused
float and float with a null subtract: same, refused
float and float with a null multiply: same, refused
integer and float add: same, Flow\ETL\Values\Column\RustColumn
integer and float subtract: same, Flow\ETL\Values\Column\RustColumn
integer and float multiply: same, Flow\ETL\Values\Column\RustColumn
integer and float with a null add: same, refused
integer and float with a null subtract: same, refused
integer and float with a null multiply: same, refused
float and integer add: same, Flow\ETL\Values\Column\RustColumn
float and integer subtract: same, Flow\ETL\Values\Column\RustColumn
float and integer multiply: same, Flow\ETL\Values\Column\RustColumn
float and integer with a null add: same, refused
float and integer with a null subtract: same, refused
float and integer with a null multiply: same, refused
modulo by positive moduli modulo: same, Flow\ETL\Values\Column\RustColumn
modulo by positive moduli with a null modulo: same, refused
modulo by any int modulo: same, refused
modulo by any int with a null modulo: same, refused
