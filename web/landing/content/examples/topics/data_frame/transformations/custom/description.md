Transformations are reusable blocks of transformations that can be applied to a data frame.  
The main goal of `Transformations` is to a gruop together a set of transformations.

Transformation needs to implement the `Transformation` interface.

```php
interface Transformation
{
    public function transform(DataFrame $dataFrame) : DataFrame;
}
```