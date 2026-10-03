# Filter

[DOC_LINK:/documentation/components/core/core.md]

To filter rows from the data frame you can use `DataFrame::filter` function.
Filter function accepts only one argument which is a `ScalarFunction` that returns `bool` value.

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

Business logic the built-in functions do not cover goes into a `ScalarFunction` that returns a boolean column:

```php
<?php

use Flow\ETL\Column\Column;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Function\{Parameter, ScalarFunction, ScalarFunctionChain};
use Flow\ETL\Rows;
use Flow\Types\Type;

use function Flow\ETL\DSL\{data_frame, from_array, ref, to_output};
use function Flow\Types\DSL\type_boolean;

final class HighValuePurchase implements ScalarFunction
{
    use ScalarFunctionChain;

    public function __construct(private readonly ScalarFunction $amount, private readonly ScalarFunction $type) {}

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

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $amounts = (new Parameter($this->amount))->asNumbers($rows, $context);
        $types = (new Parameter($this->type))->asStrings($rows, $context);
        $results = [];

        foreach ($amounts as $i => $amount) {
            $results[] = $amount > 1000 && $types[$i] === 'purchase';
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
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

> **Performance Note**: prefer built-in scalar functions when they cover the logic.

- [Until](/documentation/components/core/until.md)
