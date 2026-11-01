# Filter

[DOC_LINK:/documentation/components/core/core.md]

To filter rows of the data frame, use the `DataFrame::filter` function.
It accepts only one argument: a `ScalarFunction` that returns a `bool` value.

Example:

```php
<?php

data_frame()
    ->read(from_array([
        ['a' => 100, 'b' => 100],
        ['a' => 100, 'b' => 200]
    ]))
    ->filter(ref('b')->divide(lit(2))->equals(ref('a')))
    ->write(to_output(false))
    ->run();
```

```text
+-----+-----+
|   a |   b |
+-----+-----+
| 100 | 200 |
+-----+-----+
1 rows
```

## Custom Filter Functions

Business logic that the built-in functions do not cover goes into a `ScalarFunction` that returns a boolean column:

```php
<?php

use Flow\ETL\Foundation\Describe\{Description, Rebuild};
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Functions\{Parameter, ScalarFunction, ScalarFunctionChain};
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;

use function Flow\ETL\DSL\{data_frame, from_array, ref, to_output};
use function Flow\Types\DSL\type_boolean;

final class HighValuePurchase implements ScalarFunction
{
    use ScalarFunctionChain;

    public function __construct(private readonly ScalarFunction $amount, private readonly ScalarFunction $type) {}

    public static function fromDescription(array $data, Rebuild $rebuild): static
    {
        return new self($rebuild->of($data['amount'], ScalarFunction::class), $rebuild->of($data['type'], ScalarFunction::class));
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, ['amount' => $this->amount, 'type' => $this->type]);
    }

    public function children(): array
    {
        return [$this->amount, $this->type];
    }

    public function withChildren(array $children): static
    {
        return new self($children[0], $children[1]);
    }

    public function returns(): Type
    {
        return type_boolean();
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        $amounts = (new Parameter($this->amount))->asNumbers($rows, $backend);
        $types = (new Parameter($this->type))->asStrings($rows, $backend);
        $results = [];

        foreach ($amounts as $i => $amount) {
            $results[] = $amount > 1000 && $types[$i] === 'purchase';
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}

data_frame()
    ->read(from_array([
        ['amount' => 1500, 'type' => 'purchase'],
        ['amount' => 1500, 'type' => 'refund'],
        ['amount' => 200, 'type' => 'purchase'],
    ]))
    ->filter(new HighValuePurchase(ref('amount'), ref('type')))
    ->write(to_output(false))
    ->run();
```

> Performance Note: for logic that built-in scalar functions cover, use them.

- [Until](/documentation/components/core/until.md)
