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
    ->filter(ref('b')->divide(lit(2))->equals(lit('a')))
    ->write(to_output(false))
    ->run();
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

use function Flow\ETL\DSL\{data_frame, ref};
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

        return (new ResultColumn())->of($this, $results);
    }
}

data_frame()
    ->read($transactionExtractor)
    ->filter(new HighValuePurchase(ref('amount'), ref('type')))
    ->write($highValueTransactionLoader)
    ->run();
```

> **Performance Note**: Callback-based filtering cannot be optimized by the engine and should be used sparingly. When possible, prefer built-in scalar functions for better performance.

- [Until](/documentation/components/core/until.md)
